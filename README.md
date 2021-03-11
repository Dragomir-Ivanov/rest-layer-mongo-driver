# REST Layer MongoDB Backend

[![godoc](http://img.shields.io/badge/godoc-reference-blue.svg?style=flat)](https://godoc.org/github.com/Dragomir-Ivanov/rest-layer-mongo-driver) [![license](http://img.shields.io/badge/license-MIT-red.svg?style=flat)](https://raw.githubusercontent.com/rs/rest-layer-mongo/master/LICENSE)

This [REST Layer](https://github.com/rs/rest-layer) resource storage backend stores data in a MongoDB cluster using [mongo-driver](go.mongodb.org/mongo-driver).

## Usage

```go
import (
  "context"
  "reflect"

  "go.mongodb.org/mongo-driver/bson"
  "go.mongodb.org/mongo-driver/bson/bsontype"
  "go.mongodb.org/mongo-driver/mongo"
  "go.mongodb.org/mongo-driver/mongo/options"
  rsmongo "github.com/Dragomir-Ivanov/rest-layer-mongo-driver"
)
```

Create a mongo master session:

```go
  reg := bson.NewRegistryBuilder().
    RegisterTypeMapEntry(bsontype.DateTime, reflect.TypeOf(time.Time{})).
    RegisterTypeMapEntry(bsontype.Int32, reflect.TypeOf(1)).
    RegisterTypeMapEntry(bsontype.Array, reflect.TypeOf([]interface{}{})).
    Build()
  clientOptions := options.Client().SetRegistry(reg).ApplyURI("mongodb://localhost/")
  s, err := mongo.Connect(context.Background(), clientOptions)
```

Create a resource storage handler with a given DB/collection:

```go
s := rsmongo.NewHandler(client, "the_db", "the_collection")
```

Use this handler with a resource:

```go
index.Bind("foo", foo, s, resource.DefaultConf)
```

You may want to create a many mongo handlers as you have resources as long as you want each resources in a different collection. You can share the same `client` across all you handlers.

### Object ID

This package also provides a REST Layer [schema.Validator](https://godoc.org/github.com/rs/rest-layer/schema#Validator) for MongoDB ObjectIDs. This validator ensures proper binary serialization of the Object ID in the database for space efficiency.

You may reference this validator using [mongo.ObjectID](https://godoc.org/github.com/Dragomir-Ivanov/rest-layer-mongo-driver#ObjectID) as [schema.Field](https://godoc.org/github.com/rs/rest-layer/schema#Field).

A `mongo.NewObjectID` field hook and `mongo.ObjectIDField` helper are also provided.
