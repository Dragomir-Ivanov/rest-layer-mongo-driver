package mongo

import (
	"context"
	"strings"

	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema/query"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// getField translate a schema field into a MongoDB field:
//
//   - id -> _id with in order to tape on the mongo primary key
func getField(f string) string {
	if f == "id" {
		return "_id"
	}
	return f
}

// getQuery transform a query into a Mongo query.
func getQuery(q *query.Query) (bson.M, error) {
	return translatePredicate(q.Predicate)
}

// getSort transform a resource.Lookup into a Mongo sort list.
// If the sort list is empty, fallback to _id.
func getSort(q *query.Query) bson.D {
	if len(q.Sort) == 0 {
		return bson.D{{Key: "_id", Value: 1}}
	}

	s := bson.D{}
	for _, sort := range q.Sort {
		value := 1
		if sort.Reversed {
			value = -1
		}
		e := bson.E{Key: getField(sort.Name), Value: value}
		s = append(s, e)
	}

	// Deduplicate sort fields, keeping the last one, order maters
	// https://docs.mongodb.com/manual/reference/method/cursor.sort/#sort-duplication
	seen := map[string]bool{}
	for i := len(s) - 1; i >= 0; i-- {
		if seen[s[i].Key] {
			s = append(s[:i], s[i+1:]...)
		} else {
			seen[s[i].Key] = true
		}
	}

	return s
}

func getProjection(q *query.Query) bson.M {
	if len(q.Projection) == 0 || hasStarProjection(q) {
		return nil
	}

	p := bson.M{}
	p["_id"] = 1
	p["_etag"] = 1
	p["_updated"] = 1
	for _, field := range q.Projection {
		if field.Name == "id" {
			continue
		}

		// Extract only top level field name
		name := field.Name
		fname := strings.Split(field.Name, ".")
		if len(fname) > 1 {
			name = fname[0]
		}
		p[getField(name)] = 1
	}
	return p
}

func hasStarProjection(q *query.Query) bool {
	for _, field := range q.Projection {
		if field.Name == "*" {
			return true
		}
	}
	return false
}

func applyWindow(fo *options.FindOptions, w query.Window) *options.FindOptions {
	if w.Offset > 0 {
		fo = fo.SetSkip(int64(w.Offset))
	}
	if w.Limit > -1 {
		fo = fo.SetLimit(int64(w.Limit))
	}
	return fo
}

func selectIDs(ctx context.Context, c *mongo.Collection, cursor *mongo.Cursor) ([]interface{}, error) {
	var ids []interface{}
	tmp := struct {
		ID interface{} `bson:"_id"`
	}{}
	for cursor.Next(ctx) {
		if err := cursor.Decode(&tmp); err != nil {
			cursor.Close(ctx)
			return nil, err
		}
		ids = append(ids, tmp.ID)
	}
	if err := cursor.Close(ctx); err != nil {
		return nil, err
	}
	return ids, nil
}

func translatePredicate(q query.Predicate) (bson.M, error) {
	b := bson.M{}
	for _, exp := range q {
		switch t := exp.(type) {
		case *query.And:
			s := []bson.M{}
			for _, subExp := range *t {
				sb, err := translatePredicate(expToPredicate(subExp))
				if err != nil {
					return nil, err
				}
				s = append(s, sb)
			}
			b["$and"] = s
		case *query.Or:
			s := []bson.M{}
			for _, subExp := range *t {
				sb, err := translatePredicate(expToPredicate(subExp))
				if err != nil {
					return nil, err
				}
				s = append(s, sb)
			}
			b["$or"] = s
		case *query.ElemMatch:
			s := bson.M{}
			for _, subExp := range t.Exps {
				sb, err := translatePredicate(expToPredicate(subExp))
				if err != nil {
					return nil, err
				}
				for k, v := range sb {
					s[k] = v
				}
			}
			b[getField(t.Field)] = bson.M{"$elemMatch": s}
		case *query.In:
			b[getField(t.Field)] = bson.M{"$in": t.Values}
		case *query.NotIn:
			b[getField(t.Field)] = bson.M{"$nin": t.Values}
		case *query.Exist:
			b[getField(t.Field)] = bson.M{"$exists": true}
		case *query.NotExist:
			b[getField(t.Field)] = bson.M{"$exists": false}
		case *query.Equal:
			b[getField(t.Field)] = t.Value
		case *query.NotEqual:
			b[getField(t.Field)] = bson.M{"$ne": t.Value}
		case *query.GreaterThan:
			b[getField(t.Field)] = bson.M{"$gt": t.Value}
		case *query.GreaterOrEqual:
			b[getField(t.Field)] = bson.M{"$gte": t.Value}
		case *query.LowerThan:
			b[getField(t.Field)] = bson.M{"$lt": t.Value}
		case *query.LowerOrEqual:
			b[getField(t.Field)] = bson.M{"$lte": t.Value}
		case *query.Regex:
			if t.Negated {
				b[getField(t.Field)] = bson.M{"$not": primitive.Regex{Pattern: t.Value.String()}}
			} else {
				b[getField(t.Field)] = bson.M{"$regex": t.Value.String()}
			}
		default:
			return nil, resource.ErrNotImplemented
		}
	}
	return b, nil
}

func expToPredicate(exp query.Expression) query.Predicate {
	switch t := exp.(type) {
	case query.Predicate:
		return t
	case *query.Predicate:
		return *t
	default:
		return query.Predicate{t}
	}
}
