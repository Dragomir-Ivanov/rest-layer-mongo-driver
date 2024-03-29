// Package mongo is a REST Layer resource storage handler for MongoDB using mgo
package mongo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema/query"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// mongoItem is a bson representation of a resource.Item.
type mongoItem struct {
	ID      interface{}            `bson:"_id"`
	ETag    string                 `bson:"_etag"`
	Updated time.Time              `bson:"_updated"`
	Payload map[string]interface{} `bson:",inline"`
}

// newMongoItem converts a resource.Item into a mongoItem.
func newMongoItem(i *resource.Item) *mongoItem {
	// Filter out id from the payload so we don't store it twice
	p := map[string]interface{}{}
	for k, v := range i.Payload {
		if k != "id" {
			p[k] = v
		}
	}
	return &mongoItem{
		ID:      i.ID,
		ETag:    i.ETag,
		Updated: i.Updated,
		Payload: p,
	}
}

// newItem converts a back mongoItem into a resource.Item.
func newItem(i *mongoItem) *resource.Item {
	// If there is no field except those defined in mongoItem, Payload could be nil
	// when just fetched from the database.
	if i.Payload == nil {
		i.Payload = make(map[string]interface{})
	}
	// Add the id back (we use the same map hoping the mongoItem won't be stored back)
	i.Payload["id"] = i.ID
	item := &resource.Item{
		ID:      i.ID,
		ETag:    i.ETag,
		Updated: i.Updated,
		Payload: i.Payload,
	}

	if item.ETag == "" {
		if v, ok := i.ID.(primitive.ObjectID); ok {
			item.ETag = "p-" + v.Hex()
		} else {
			item.ETag = "p-" + fmt.Sprint(i.ID)
		}
	}
	return item
}

// Handler handles resource storage in a MongoDB collection.
type Handler func(ctx context.Context) (*mongo.Collection, error)

// NewHandler creates an new mongo handler
func NewHandler(s *mongo.Client, db, collection string) Handler {
	c := func() *mongo.Collection {
		return s.Database(db).Collection(collection)
	}
	return func(ctx context.Context) (*mongo.Collection, error) {
		return c(), nil
	}
}

// C returns the mongo collection managed by this storage handler
// from a Copy() of the mgo session.
func (m Handler) c(ctx context.Context) (*mongo.Collection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c, err := m(ctx)
	if err != nil {
		return nil, err
	}
	// With mgo, session.Copy() pulls a connection from the connection pool
	// s := c.Database.Session.Copy()
	// Ensure safe mode is enabled in order to get errors
	// s.EnsureSafe(&mgo.Safe{})
	// Set a timeout to match the context deadline if any
	// if deadline, ok := ctx.Deadline(); ok {
	// 	timeout := deadline.Sub(time.Now())
	// 	if timeout <= 0 {
	// 		timeout = 0
	// 	}
	// 	s.SetSocketTimeout(timeout)
	// 	s.SetSyncTimeout(timeout)
	// }
	// c.Database.Session = s
	return c, nil
}

// close returns a mgo.Collection's session to the connection pool.
func (m Handler) close(c *mongo.Collection) {
	// c.Database.Session.Close()
}

func isDup(err error) bool {
	{
		var e mongo.WriteException
		if errors.As(err, &e) {
			for _, we := range e.WriteErrors {
				if we.Code == 11000 {
					return true
				}
			}
		}
	}
	{
		var e mongo.BulkWriteException
		if errors.As(err, &e) {
			for _, we := range e.WriteErrors {
				if we.Code == 11000 {
					return true
				}
			}
		}
	}

	return false
}

// Insert inserts new items in the mongo collection.
func (m Handler) Insert(ctx context.Context, items []*resource.Item) error {
	mItems := make([]interface{}, len(items))
	for i, item := range items {
		mItems[i] = newMongoItem(item)
	}
	c, err := m.c(ctx)
	if err != nil {
		return err
	}
	defer m.close(c)
	_, err = c.InsertMany(ctx, mItems)
	if isDup(err) {
		// Duplicate ID key
		err = resource.ErrConflict
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// Update replace an item by a new one in the mongo collection.
func (m Handler) Update(ctx context.Context, item *resource.Item, original *resource.Item) error {
	mItem := newMongoItem(item)
	c, err := m.c(ctx)
	if err != nil {
		return err
	}
	defer m.close(c)
	s := bson.M{"_id": original.ID}
	if strings.HasPrefix(original.ETag, "p-") {
		// If the original ETag is in "p-[id]" format,
		// then _etag field must be absent from the resource in DB
		s["_etag"] = bson.M{"$exists": false}
	} else {
		s["_etag"] = original.ETag
	}
	info, err := c.ReplaceOne(ctx, s, mItem)
	if err != nil {
		return err
	}
	if info.MatchedCount == 0 {
		// Determine if the item is not found or if the item is found but etag missmatch
		info := c.FindOne(ctx, bson.M{"_id": original.ID})
		if err != nil {
			// The find returned an unexpected err, just forward it with no mapping
		} else if info.Err() == mongo.ErrNoDocuments {
			err = resource.ErrNotFound
		} else if ctx.Err() != nil {
			err = ctx.Err()
		} else {
			// If the item were found, it means that its etag didn't match
			err = resource.ErrConflict
		}
	}
	return err
}

// Delete deletes an item from the mongo collection.
func (m Handler) Delete(ctx context.Context, item *resource.Item) error {
	c, err := m.c(ctx)
	if err != nil {
		return err
	}
	defer m.close(c)
	s := bson.M{"_id": item.ID}
	if strings.HasPrefix(item.ETag, "p-") {
		// If the item ETag is in "p-[id]" format,
		// then _etag field must be absent from the resource in DB
		s["_etag"] = bson.M{"$exists": false}
	} else {
		s["_etag"] = item.ETag
	}
	info, err := c.DeleteOne(ctx, s)
	if err != nil {
		return err
	}
	if info.DeletedCount == 0 {
		// Determine if the item is not found or if the item is found but etag missmatch
		info := c.FindOne(ctx, bson.M{"_id": item.ID})
		if err != nil {
			// The find returned an unexpected err, just forward it with no mapping
		} else if info.Err() == mongo.ErrNoDocuments {
			err = resource.ErrNotFound
		} else if ctx.Err() != nil {
			err = ctx.Err()
		} else {
			// If the item were found, it means that its etag didn't match
			err = resource.ErrConflict
		}
	}
	return err
}

// Clear clears all items from the mongo collection matching the query. Note
// that when q.Window != nil, the current implementation may error if the BSON
// encoding of all matching IDs according to the q.Window length gets close to
// the maximum document size in MongDB (usually 16MiB):
// https://docs.mongodb.com/manual/reference/limits/#bson-documents
func (m Handler) Clear(ctx context.Context, q *query.Query) (int, error) {
	// When not applying windowing, qry will be passed directly to RemoveAll.
	qry, err := getQuery(q)
	if err != nil {
		return 0, err
	}

	c, err := m.c(ctx)
	if err != nil {
		return 0, err
	}
	defer m.close(c)

	if q.Window != nil {
		// RemoveAll does not allow skip and limit to be set. To workaround
		// this we do an additional pre-query to retrieve a sorted and sliced
		// list of the IDs for all items to be deleted.
		//
		// This solution does not handle the case where a query containg all
		// IDs is larger than the maximum BSON document size in MongoDB:
		// https://docs.mongodb.com/manual/reference/limits/#bson-documents
		findOptions := options.Find()
		findOptions.SetSort(getSort(q))
		findOptions = applyWindow(findOptions, *q.Window)
		findOptions.SetProjection(bson.M{"_id": 1})
		cursor, err := c.Find(ctx, qry, findOptions)
		if err != nil {
			return 0, err
		}

		if ids, err := selectIDs(ctx, c, cursor); err == nil {
			qry = bson.M{"_id": bson.M{"$in": ids}}
		} else {
			return 0, err
		}
	}

	// We handle the potential of partial failure by returning both the number
	// of removed items and an error, if both are present.
	info, err := c.DeleteMany(ctx, qry)
	if err == nil {
		err = ctx.Err()
	}
	if info == nil {
		return 0, err
	}
	return int(info.DeletedCount), err
}

// Find items from the mongo collection matching the provided query.
func (m Handler) Find(ctx context.Context, q *query.Query) (*resource.ItemList, error) {
	// MongoDB will return all records on Limit=0. Workaround that behavior.
	// https://docs.mongodb.com/manual/reference/method/cursor.limit/#zero-value
	if q.Window != nil && q.Window.Limit == 0 {
		n, err := m.Count(ctx, q)
		if err != nil {
			return nil, err
		}
		list := &resource.ItemList{
			Total: n,
			Limit: q.Window.Limit,
			Items: []*resource.Item{},
		}
		return list, err
	}

	qry, err := getQuery(q)
	if err != nil {
		return nil, err
	}

	findOptions := options.Find()
	findOptions.SetSort(getSort(q))
	findOptions.SetProjection(getProjection(q))

	c, err := m.c(ctx)
	if err != nil {
		return nil, err
	}
	defer m.close(c)

	limit := -1
	if q.Window != nil {
		findOptions = applyWindow(findOptions, *q.Window)
		limit = q.Window.Limit
	}

	// Apply context deadline if any
	if dl, ok := ctx.Deadline(); ok {
		findOptions.SetMaxTime(time.Until(dl))
	}

	// Perform request
	cursor, err := c.Find(ctx, qry, findOptions)
	if err != nil {
		return nil, err
	}
	// Total is set to -1 because we have no easy way with MongoDB to to compute
	// this value without performing two requests.
	list := &resource.ItemList{
		Total: -1,
		Limit: limit,
		Items: []*resource.Item{},
	}

	for cursor.Next(ctx) {
		var mItem mongoItem
		if err := cursor.Decode(&mItem); err != nil {
			return nil, err
		}
		// Check if context is still ok before to continue
		if err = ctx.Err(); err != nil {
			// TODO bench this as net/context is using mutex under the hood
			cursor.Close(ctx)
			return nil, err
		}
		list.Items = append(list.Items, newItem(&mItem))
	}
	if err := cursor.Close(ctx); err != nil {
		return nil, err
	}
	// If the number of returned elements is lower than requested limit, or no
	// limit is requested, we can deduce the total number of element for free.
	if limit < 0 || len(list.Items) < limit {
		if q.Window != nil && q.Window.Offset > 0 {
			if len(list.Items) > 0 {
				list.Total = q.Window.Offset + len(list.Items)
			}
			// If there are no items returned when Offset > 0, we may be out-of-bounds,
			// and therefore cannot deduce the total count of items.
		} else {
			list.Total = len(list.Items)
		}
	}
	return list, err
}

func (m Handler) Reduce(ctx context.Context, q *query.Query, reducer func(item *resource.Item) error) error {
	// MongoDB will return all records on Limit=0. Workaround that behavior.
	// https://docs.mongodb.com/manual/reference/method/cursor.limit/#zero-value
	if q.Window != nil && q.Window.Limit == 0 {
		return nil
	}

	qry, err := getQuery(q)
	if err != nil {
		return err
	}

	findOptions := options.Find()
	findOptions.SetSort(getSort(q))
	findOptions.SetProjection(getProjection(q))

	c, err := m.c(ctx)
	if err != nil {
		return err
	}
	defer m.close(c)

	if q.Window != nil {
		findOptions = applyWindow(findOptions, *q.Window)
	}

	// Apply context deadline if any
	if dl, ok := ctx.Deadline(); ok {
		findOptions.SetMaxTime(time.Until(dl))
	}

	// Perform request
	cursor, err := c.Find(ctx, qry, findOptions)
	if err != nil {
		return err
	}

	for cursor.Next(ctx) {
		var mItem mongoItem
		if err := cursor.Decode(&mItem); err != nil {
			return err
		}
		// Check if context is still ok before to continue
		if err = ctx.Err(); err != nil {
			// TODO bench this as net/context is using mutex under the hood
			cursor.Close(ctx)
			return err
		}
		err = reducer(newItem(&mItem))
		if err != nil {
			return err
		}
	}

	return cursor.Close(ctx)
}

// Count counts the number items matching the lookup filter
func (m Handler) Count(ctx context.Context, query *query.Query) (int, error) {
	q, err := getQuery(query)
	if err != nil {
		return -1, err
	}
	c, err := m.c(ctx)
	if err != nil {
		return -1, err
	}
	defer m.close(c)

	countOptions := options.Count()
	// Apply context deadline if any
	if dl, ok := ctx.Deadline(); ok {
		countOptions.SetMaxTime(time.Until(dl))
	}

	n, err := c.CountDocuments(ctx, q, countOptions)
	if err != nil {
		return -1, err
	}

	return int(n), nil
}
