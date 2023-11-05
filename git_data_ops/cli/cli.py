import click
from git_data_ops.src.core.config import PostgresConfig
from git_data_ops.src.database.postgres import PostgresConnector
from git_data_ops.src.core.config import ConfigManager


@click.group()
def cli():
    pass


@cli.command()
@click.option("--list-all", is_flag=True, default=False)
def list_databases(list_all: bool = False):
    context = ConfigManager()
    active_database = context.config["database"]
    active_database_type = context.config["databases"][active_database]["type"]

    if active_database_type == "postgresql":
        db_config: PostgresConfig = PostgresConfig(active_database)
        db_connector: PostgresConnector = PostgresConnector(db_config)
    else:
        print("No database config type found")

    databases = db_connector.list_databases()

    if not list_all:
        databases = databases[:10]
    for db in databases:
        print(db)
    if len(databases) > 10:
        print("Some databases were emitted. Use '--list-all' to list all databases")


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
