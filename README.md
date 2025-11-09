
# MongoDB quickstart (optional, if you want a *real* NoSQL source)

## Seed
```bash
# Start MongoDB locally, then insert documents
mongoimport --uri "mongodb://localhost:27017"   --db ds2002 --collection providers   --jsonArray --file mongo/providers_array.json
```

If your mongoimport expects an array, use the provided `providers_array.json`.
If not, you can also insert documents manually from `seed_providers.json`.

## Example aggregation (sanity check)
```javascript
db.providers.aggregate([
  { $group: { _id: "$network_tier", regions: { $addToSet: "$region" }, count: { $sum: 1 } } }
])
```

## Using Mongo for enrichment
Set `USE_MONGO_FOR_REGION=1` and configure `MONGO_URI`, `MONGO_DB`, `MONGO_COL`.
Then run `python etl/etl_pipeline_mysql.py` and regions will be pulled from MongoDB.
