#!/usr/bin/env python3
import argparse
import json
import logging
import resource
import sys
import time
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

DEFAULT_INPUT = "hdfs://namenode:9000/data/online-retail/online_retail_II.csv"
DEFAULT_OUTPUT_DIR = "/workspace/logs"


def setup_logger(log_file):
    logger = logging.getLogger("online-retail-optimized")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


def build_spark(app_name, master):
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "12")
        .config("spark.default.parallelism", "12")
        .config("spark.eventLog.enabled", "false")
        .getOrCreate()
    )


def get_executor_memory_status(spark):
    status_map = spark.sparkContext._jsc.sc().getExecutorMemoryStatus()
    entries = []
    iterator = status_map.keysIterator()
    while iterator.hasNext():
        host_port = iterator.next()
        mem_tuple = status_map.apply(host_port)
        max_mem = int(mem_tuple._1())
        remaining_mem = int(mem_tuple._2())
        used_mem = max_mem - remaining_mem
        entries.append(
            {
                "executor": host_port,
                "max_mb": round(max_mem / 1024 / 1024, 2),
                "used_mb": round(used_mem / 1024 / 1024, 2),
                "remaining_mb": round(remaining_mem / 1024 / 1024, 2),
            }
        )
    return sorted(entries, key=lambda item: str(item["executor"]))


def get_driver_memory_mb():
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 2)


def collect_job_stage_info(spark, group_id):
    tracker = spark.sparkContext.statusTracker()
    job_ids = list(tracker.getJobIdsForGroup(group_id))
    jobs = []
    for job_id in job_ids:
        job_info = tracker.getJobInfo(job_id)
        stage_ids = []
        stage_summaries = []
        if job_info:
            stage_ids = [int(stage_id) for stage_id in job_info.stageIds]
            for stage_id in stage_ids:
                stage_info = tracker.getStageInfo(stage_id)
                if not stage_info:
                    continue
                stage_summaries.append(
                    {
                        "stage_id": int(stage_info.stageId),
                        "name": stage_info.name,
                        "num_tasks": int(stage_info.numTasks),
                    }
                )
        jobs.append({"job_id": int(job_id), "stage_ids": stage_ids, "stages": stage_summaries})
    return {"job_ids": job_ids, "jobs": jobs}


def run_action(spark, logger, label, group_id, action):
    logger.info("Action started: %s", label)
    spark.sparkContext.setJobGroup(group_id, label)
    started_at = time.perf_counter()
    result = action()
    duration = round(time.perf_counter() - started_at, 3)

    job_stage_info = collect_job_stage_info(spark, group_id)
    memory_snapshot = {
        "driver_max_rss_mb": get_driver_memory_mb(),
        "executors": get_executor_memory_status(spark),
    }

    logger.info("Action finished: %s | duration=%.3fs", label, duration)
    logger.info("Job IDs: %s", job_stage_info["job_ids"])
    for job in job_stage_info["jobs"]:
        logger.info("Job %s stages=%s", job["job_id"], job["stage_ids"])
        for stage in job["stages"]:
            logger.info("Stage %s | %s | tasks=%s", stage["stage_id"], stage["name"], stage["num_tasks"])
    for executor in memory_snapshot["executors"]:
        logger.info(
            "Executor %s | used=%s MB | remaining=%s MB | max=%s MB",
            executor["executor"],
            executor["used_mb"],
            executor["remaining_mb"],
            executor["max_mb"],
        )
    logger.info("Driver max RSS: %s MB", memory_snapshot["driver_max_rss_mb"])

    return result, {
        "label": label,
        "duration_sec": duration,
        "job_stage_info": job_stage_info,
        "memory_snapshot": memory_snapshot,
    }


def build_schema():
    return StructType(
        [
            StructField("Invoice", StringType(), True),
            StructField("StockCode", StringType(), True),
            StructField("Description", StringType(), True),
            StructField("Quantity", IntegerType(), True),
            StructField("InvoiceDate", TimestampType(), True),
            StructField("Price", DoubleType(), True),
            StructField("Customer ID", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("SourceSheet", StringType(), True),
        ]
    )


def optimize_df(df):
    return (
        df.withColumnRenamed("Invoice", "invoice_no")
        .withColumnRenamed("StockCode", "stock_code")
        .withColumnRenamed("Description", "description")
        .withColumnRenamed("Quantity", "quantity")
        .withColumnRenamed("InvoiceDate", "invoice_ts")
        .withColumnRenamed("Price", "price")
        .withColumnRenamed("Customer ID", "customer_id")
        .withColumnRenamed("Country", "country")
        .withColumnRenamed("SourceSheet", "source_sheet")
        .filter(
            (F.col("quantity").isNotNull())
            & (F.col("price").isNotNull())
            & (F.col("quantity") > 0)
            & (F.col("price") > 0)
            & F.col("country").isNotNull()
        )
        .withColumn("revenue", F.round(F.col("quantity") * F.col("price"), 2))
        .repartition(12, "country")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--app-name", default="online-retail-optimized")
    parser.add_argument("--run-label", default="1DataNode_opt")
    return parser.parse_args()


def main():
    args = parse_args()
    output_dir = Path(DEFAULT_OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    log_file = output_dir / f"{args.run_label}.log"
    metrics_file = output_dir / f"{args.run_label}_metrics.json"

    logger = setup_logger(log_file)
    logger.info("Spark optimized app started")
    logger.info("Input path: %s", args.input)
    logger.info("Master: %s", "spark://spark-master:7077")

    spark = build_spark(args.app_name, "spark://spark-master:7077")
    spark.sparkContext.setLogLevel("ERROR")
    app_started_at = time.perf_counter()

    try:
        read_started_at = time.perf_counter()
        raw_df = spark.read.option("header", True).schema(build_schema()).csv(args.input)
        read_definition_sec = round(time.perf_counter() - read_started_at, 3)
        logger.info("DataFrame created with explicit schema in %.3fs", read_definition_sec)

        optimized_df = optimize_df(raw_df)
        cache_rows, cache_metrics = run_action(
            spark,
            logger,
            "materialize cached dataframe",
            "cache-materialization",
            lambda: optimized_df.count(),
        )
        logger.info("Rows in optimized cached dataframe: %s", cache_rows)

        country_metrics, country_metrics_log = run_action(
            spark,
            logger,
            "top countries by revenue",
            "country-revenue-opt",
            lambda: [
                row.asDict()
                for row in optimized_df.groupBy("country")
                .agg(
                    F.count("*").alias("orders"),
                    F.round(F.sum("revenue"), 2).alias("total_revenue"),
                    F.round(F.avg("revenue"), 2).alias("avg_revenue"),
                )
                .orderBy(F.col("total_revenue").desc())
                .limit(10)
                .collect()
            ],
        )
        logger.info("Top countries by revenue: %s", country_metrics)

        monthly_metrics, monthly_metrics_log = run_action(
            spark,
            logger,
            "monthly revenue by source sheet",
            "monthly-revenue-opt",
            lambda: [
                row.asDict()
                for row in optimized_df.withColumn("invoice_month", F.date_format("invoice_ts", "yyyy-MM"))
                .groupBy("source_sheet", "invoice_month")
                .agg(F.count("*").alias("orders"), F.round(F.sum("revenue"), 2).alias("monthly_revenue"))
                .orderBy("source_sheet", "invoice_month")
                .limit(12)
                .collect()
            ],
        )
        logger.info("Monthly revenue sample: %s", monthly_metrics)

        metrics = {
            "app_name": args.app_name,
            "run_label": args.run_label,
            "master": spark.sparkContext.master,
            "input": args.input,
            "dataframe_creation_sec": read_definition_sec,
            "total_runtime_sec": round(time.perf_counter() - app_started_at, 3),
            "row_count": cache_rows,
            "actions": [cache_metrics, country_metrics_log, monthly_metrics_log],
            "samples": {
                "top_countries_by_revenue": country_metrics,
                "monthly_revenue_sample": monthly_metrics,
            },
            "optimizations": {
                "explicit_schema": True,
                "repartition": "repartition(12, 'country')",
                "persistence": "MEMORY_AND_DISK",
            },
        }
        metrics_file.write_text(json.dumps(metrics, indent=2, ensure_ascii=True), encoding="utf-8")
        logger.info("Metrics written to %s", metrics_file)
        logger.info("Log written to %s", log_file)
        logger.info("Spark optimized app finished successfully")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
