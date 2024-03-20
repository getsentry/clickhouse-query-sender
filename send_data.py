from datetime import datetime, timedelta
from clickhouse_driver import Client
import click
from numpy import random


@click.command()
@click.option(
    "--host",
    default="127.0.0.1",
    show_default=True,
    help="The host address for clickhouse.",
)
@click.option(
    "--port",
    default=9000,
    show_default=True,
    help="The host address for clickhouse.",
)
@click.option(
    "--count",
    default=100,
    show_default=True,
    help="The number of metrics per 10s bucket",
)
@click.option(
    "--interval",
    default=89,
    show_default=True,
    help="The number of days of data to generate",
)
def main(host, port, count, interval):
    print(f"Creating client connection to {host}:{port}")

    client = Client(
        host=host,
        port=port,
    )
    columns = [
        "use_case_id",
        "org_id",
        "project_id",
        "metric_id",
        "timestamp",
        "retention_days",
        "tags.key",
        "tags.indexed_value",
        "tags.raw_value",
        "distribution_values",
        "metric_type",
        "materialization_version",
        "timeseries_id",
        "partition",
        "offset",
        "granularities",
        "enable_histogram",
        "decasecond_retention_days",
        "min_retention_days",
        "hr_retention_days",
        "day_retention_days",
    ]

    num_rows_inserted = 0

    for time in daterange(datetime.now() - timedelta(days=interval), datetime.now()):
        num_rows_inserted += client.execute(
            f"INSERT INTO default.generic_metric_distributions_raw_local ({', '.join(columns)}) VALUES",
            [make_dist_payload(time) for _ in range(count)],
        )
        print(f"Date timestamp: {time}, inserted: {num_rows_inserted}", end="\r")
    print("Done")


def daterange(start_date: datetime, end_date: datetime):
    for n in range(int((end_date - start_date).total_seconds()) // 10):
        yield start_date + timedelta(seconds=n * 10)


def rand_bucket_len(intervals):
    idx = random.randint(0, len(intervals))
    return random.randint(1 if idx == 0 else intervals[idx - 1], intervals[idx])


def make_dist_payload(timestamp):
    padding = 2**32
    return {
        "use_case_id": (
            use_case_id := random.choice(
                ["qwertyu", "asdfghj", "zxcvbnm", "iop[]kl;"], p=[0.45, 0.4, 0.08, 0.07]
            )
        ),
        "org_id": (org_id := random.choice(100_000) + padding),
        "project_id": (proj_id := random.choice(170_000) + padding),
        "metric_id": (metric_id := random.choice(100) + padding),
        "timestamp": timestamp,
        "retention_days": random.choice([90, 7], p=[0.999, 0.001]),
        "tags.key": [key + padding for key in random.choice(32, size=7, replace=False)],
        "tags.indexed_value": (
            indexed_values := [key + padding for key in random.choice(650, size=7)]
        ),
        "tags.raw_value": [
            f"{use_case_id}:{org_id}:{proj_id}:{metric_id}:{str(e)}"
            for e in indexed_values
        ],
        "distribution_values": [
            random.random() for _ in range(rand_bucket_len([5, 100, 500, 50_000]))
        ],
        "metric_type": "distribution",
        "materialization_version": 2,
        "timeseries_id": 1,
        "partition": 0,
        "offset": 0,
        "granularities": [0, 1, 2, 3],
        "enable_histogram": 0,
        "decasecond_retention_days": 7,
        "min_retention_days": 90,
        "hr_retention_days": 90,
        "day_retention_days": 90,
    }


if __name__ == "__main__":
    main()
