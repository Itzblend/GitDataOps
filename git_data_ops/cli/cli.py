import click
import os
from git_data_ops.src.core.config import PostgresConfig
from git_data_ops.src.database.postgres import PostgresConnector
from git_data_ops.src.core.config import ConfigManager
from git_data_ops.src.core.context import Repository
from git_data_ops.src.database.database_factory import DatabaseFactory


@click.group()
def cli():
    pass


@cli.group()
def database():
    pass


@cli.group()
def git():
    pass


@database.command()
@click.option("--list-all", is_flag=True, default=False)
def list_databases(list_all: bool = False):
    context = ConfigManager()
    active_database = context.config["database"]
    db_config: dict = context.get_database_config(active_database)

    db_connector = DatabaseFactory.create_database(db_config=db_config)
    databases = db_connector.list_databases()

    if not list_all:
        databases = databases[:10]
    for db in databases:
        print(db)
    if len(databases) > 10:
        print("Some databases were emitted. Use '--list-all' to list all databases")


@database.command()
@click.option("--database", "-d")
def dump_database(database):

    context = ConfigManager()

    if not database:
        database = context.config["database"]
        _proceed = input(f"No database supplied. Proceed with '{database}' [Y/n] ")

        if _proceed.lower() not in ["y", "yes"]:
            return

    db_config: dict = context.get_database_config("local-postgres")
    db_connector = DatabaseFactory.create_database(db_config=db_config)

    db_connector.dump_database()


@database.command()
@click.option(
    "--dump_dir", default="dumps", help="Directory holding your database dumps"
)
@click.option("--database", "-d", help="An empty database to recover into")
def recover_database(dump_dir: str = "dumps", database: str = ""):
    if not database:
        print("Provide a empty database")
        return

    context = ConfigManager()
    dumps = context.list_latest_dumps(directory=dump_dir)

    print("Recent dumps:")
    for idx, dump in enumerate(dumps):
        print(f"{idx}. {dump[0]} - {dump[1].isoformat()}")

    # Choose dump interactively
    choice_idx = int(input(f"Which dump to recover? [0-{len(dumps) - 1}] "))
    dump_filename = os.path.join(dump_dir, dumps[choice_idx][0])

    active_database = context.config["database"]
    db_config: dict = context.get_database_config(active_database)

    db_connector = DatabaseFactory.create_database(db_config=db_config)
    db_connector.recover_database(database=database, dump_filename=dump_filename)


@git.command()
@click.option("--list-all", is_flag=True, default=False)
def list_branches(list_all: bool = False):
    repo = Repository()
    branches = repo.list_branches()

    if not list_all:
        branches = branches[:10]
    for branch in branches:
        print(branch)
    if len(branches) > 10:
        print("Some branches were emitted. Use '--list-all' to list all branches")


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
