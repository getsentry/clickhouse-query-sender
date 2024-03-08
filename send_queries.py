import click
import csv
import itertools
import sys
from pprint import pprint
from clickhouse_driver import Client


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
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    show_default=True,
    help="The host address for clickhouse.",
)
@click.option(
    "--lines",
    "-n",
    default=-1,
    show_default=True,
    help="The number of queries to send. (-1 to send all)",
)
@click.argument("filename", type=click.Path(exists=True))
def main(filename, host, port, verbose, lines):
    print(f"Creating client connection to {host}:{port}")

    client = Client(
        host=host,
        port=port,
    )

    with open(filename, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        if lines > -1:
            reader = itertools.islice(reader, lines)
        for row in reader:
            try:
                client.execute(row["sql"])
            except Exception as e:
                print(f"Unable to execute query:\n{row['sql']}", file=sys.stderr)
                print(f"Exception:\n{e}", file=sys.stderr)
            if verbose:
                pprint(
                    {
                        "query": row["sql"],
                        "Rows:": client.last_query.progress.rows,
                        "Bytes:": client.last_query.progress.bytes,
                        "server-side elapsed_ns": client.last_query.progress.elapsed_ns,
                    }
                )
            else:
                print(
                    f"\"{row['sql']}\",{client.last_query.progress.rows},{client.last_query.progress.bytes},{client.last_query.progress.elapsed_ns}"
                )


if __name__ == "__main__":
    main()
