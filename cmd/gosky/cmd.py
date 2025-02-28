import polars as pl

file = "~/utkarsh/solo/indigo/cmd/gosky/posts.json"

df = pl.read_ndjson(file, infer_schema_length=None)

df1 = (
    df.filter(pl.col("reply").is_null() | ~pl.col("reason").is_null())
    .unnest("post")
    .select("author", "reason")
    .unnest("reason")
    .select("author", "by")
    .unnest("author")
    .select(pl.col("displayName").alias("author"), "by")
    .unnest("by")
    .select("author", pl.col("displayName").alias("reposter"))
)

print(
    df1.select(
        pl.when(pl.col("reposter").is_null())
        .then(pl.col("author"))
        .otherwise(pl.col("reposter"))
    )
    .group_by("author")
    .len()
    .sort("len")
)
