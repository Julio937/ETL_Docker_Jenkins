import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name="kaggle-etl"):
    return (SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.sql.session.timeZone","UTC")
            .getOrCreate())

def read_input(spark, input_dir):
    path = os.path.join(input_dir, "*.csv")
    df = (spark.read
          .option("header", True)
          .option("inferSchema", True)
          .option("multiLine", True)
          .option("escape", "\"")
          .csv(path))
    if df.rdd.isEmpty():
        raise RuntimeError(f"No se encontraron CSV en {path}")
    return df

def autodetect_time_col(df):
    candidates = ["Start_Time","End_Time","pickup_datetime","dropoff_datetime","datetime","timestamp","date","time"]
    lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower:
            return lower[c.lower()]
    for c in df.columns:
        lc = c.lower()
        if "date" in lc or "time" in lc:
            return c
    return None

def autodetect_cat_col(df, exclude=None, max_distinct=5000):
    str_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, T.StringType)]
    if exclude:
        str_cols = [c for c in str_cols if c != exclude]
    for c in str_cols:
        try:
            if df.select(c).na.drop().agg(F.countDistinct(c)).first()[0] <= max_distinct:
                return c
        except Exception:
            pass
    return str_cols[0] if str_cols else None

def clean_and_cast(df, time_col=None):
    out = df
    for f in out.schema.fields:
        if isinstance(f.dataType, T.StringType):
            out = out.withColumn(f.name, F.trim(F.col(f.name)))
            out = out.withColumn(f.name, F.when(F.col(f.name).isNull(), F.lit("UNKNOWN")).otherwise(F.col(f.name)))
    if time_col and time_col in out.columns:
        out = out.withColumn("ts", F.to_timestamp(F.col(time_col)))
    else:
        out = out.withColumn("ts", F.lit(None).cast(T.TimestampType()))
    out = (out
           .withColumn("date", F.to_date("ts"))
           .withColumn("hour", F.hour("ts"))
           .withColumn("weekday", F.date_format("ts","E")))
    out = out.dropDuplicates()
    return out

def metric_time(df):
    if "ts" not in df.columns:
        return None
    return (df.where(F.col("ts").isNotNull())
              .groupBy(F.to_date("ts").alias("date"))
              .agg(F.count(F.lit(1)).alias("events"))
              .orderBy("date"))

def metric_top_categories(df, cat_col):
    if not cat_col or cat_col not in df.columns:
        return None
    return (df.groupBy(cat_col)
              .agg(F.count(F.lit(1)).alias("events"))
              .orderBy(F.desc("events"))
              .limit(10))

def write_parquet(df, path):
    (df.coalesce(1)
       .write.mode("overwrite")
       .parquet(path))

def run(input_dir, output_dir, time_col=None, cat_col=None):
    spark = build_spark()
    try:
        raw = read_input(spark, input_dir)
        time_col = time_col or autodetect_time_col(raw)
        cat_col  = cat_col  or autodetect_cat_col(raw, exclude=time_col)
        print(f"[INFO] time_col={time_col}  cat_col={cat_col}")
        df = clean_and_cast(raw, time_col=time_col)
        os.makedirs(output_dir, exist_ok=True)

        mt = metric_time(df)
        if mt is not None:
            write_parquet(mt, os.path.join(output_dir, "metric_by_date.parquet"))
            print("[OK] métricas temporales -> output/metric_by_date.parquet")
        else:
            print("[WARN] métrica temporal no disponible.")

        mc = metric_top_categories(df, cat_col)
        if mc is not None:
            write_parquet(mc, os.path.join(output_dir, "top_categories.parquet"))
            print("[OK] métricas categóricas -> output/top_categories.parquet")
        else:
            print("[WARN] métrica categórica no disponible.")

        write_parquet(df.limit(1000), os.path.join(output_dir, "clean_sample.parquet"))
    finally:
        spark.stop()

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="ETL PySpark para dataset de Kaggle/CSV")
    p.add_argument("--input-dir", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--time-col", default=None)
    p.add_argument("--category-col", default=None)
    args = p.parse_args()
    run(args.input_dir, args.output_dir, args.time_col, args.category_col)
