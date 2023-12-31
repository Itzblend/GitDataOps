import click
import os
from git_data_ops.src.core.config import PostgresConfig
from git_data_ops.src.database.postgres import PostgresConnector
from git_data_ops.src.core.config import ConfigManager
from git_data_ops.src.core.context import Repository
from git_data_ops.src.database.database_factory import DatabaseFactory
from datetime import datetime, timedelta
from git_data_ops.src.core.util import dates_to_diff, rank_by_date, init_project


@click.group()
def cli():
    pass


@cli.group()
@click.pass_context
def database(ctx):
    ctx.ensure_object(dict)
    context = ConfigManager()
    ctx.obj["active_database"] = context.config["database"]
    ctx.obj["active_branch"] = context.config["branch"]
    ctx.obj["db_config"]: dict = context.get_database_config(
        database_identifier=ctx.obj["active_database"], branch=ctx.obj["active_branch"]
    )


@cli.group()
def git():
    pass


@cli.command()
@click.option("--project-root-dir", default="./")
def init(project_root_dir: str = "./"):
    init_project(project_root_dir=project_root_dir)


@database.command()
@click.option("-b", "--branch", "branch")
@click.option("-e", "--environment", "environment", default=None)
@click.pass_context
def branch(ctx, branch, environment: str = None):
    print(f"Creating branch {branch} into {environment}")


@database.command()
@click.option(
    "--table_identifier", default="", help="Table in form of 'schema_name.table_name'"
)
@click.pass_context
def generate_ddl(ctx, table_identifier, default=""):
    db_connector = DatabaseFactory.create_database(ctx.obj["db_config"])
    _ = db_connector.get_all_tables()

    if table_identifier and isinstance(table_identifier, str):
        db_connector.create_ddl_script(table_identifier)
    else:
        db_connector.create_ddl_script()


@database.command()
@click.option("--list-all", is_flag=True, default=False)
@click.pass_context
def list_databases(ctx, list_all: bool = False):
    db_connector = DatabaseFactory.create_database(db_config=ctx.obj["db_config"])
    databases = db_connector.list_databases()

    if not list_all:
        databases = databases[:10]
    for db in databases:
        print(db)
    if len(databases) > 10:
        print("Some databases were emitted. Use '--list-all' to list all databases")


@database.command()
@click.option("--database", "-d")
@click.pass_context
def dump_database(ctx, database):
    if not database:
        database = ctx.obj["active_database"]
        _proceed = input(f"No database supplied. Proceed with '{database}' [Y/n] ")

        if _proceed.lower() not in ["y", "yes"]:
            return

    db_connector = DatabaseFactory.create_database(db_config=ctx.obj["db_config"])

    db_connector.dump_database()


@database.command()
@click.option(
    "--dump_dir", default="dumps", help="Directory holding your database dumps"
)
@click.option("--database", "-d", help="An empty database to recover into")
@click.pass_context
def recover_database(ctx, dump_dir: str = "dumps", database: str = ""):
    if not database:
        print("Provide a empty database")
        return

    context = ConfigManager()
    dumps = context.list_latest_dumps(directory=dump_dir)
    dumps_date_ranks_dict = rank_by_date([dump[1] for dump in dumps], reverse=True)

    print("Recent dumps:")
    for idx, dump in enumerate(dumps):
        filepath = dump[0]
        timestamp = dump[1]
        print(
            f"{idx}. {filepath} - {timestamp.isoformat()} - {dates_to_diff(datetime.now(), timestamp)}"
        )

    # Choose dump interactively
    choice_idx = int(input(f"Which dump to recover? [0-{len(dumps) - 1}] "))
    dump_filename = os.path.join(dump_dir, dumps[choice_idx][0])

    repo = Repository()
    branches = repo.list_branches()

    for idx, branch in enumerate(branches):
        print(f"{idx}. {branch}")

    # Choose branch interactively
    branch_choice_idx = int(input(f"Which branch to use? [0-{len(branches) - 1}] "))
    branch = branches[branch_choice_idx]

    feature_db_name = database + "-" + branch

    print(f"Creating database branch {feature_db_name}")

    db_connector = DatabaseFactory.create_database(db_config=ctx.obj["db_config"])
    db_connector.recover_database(database=feature_db_name, dump_filename=dump_filename)


@git.command()
@click.option("--list-all", is_flag=True, default=False)
def list_branches(list_all: bool = False):
    repo = Repository()
    branches = repo.list_branches()

    if len(branches) == 0:
        print(f"{repo.working_dir} has no branches")
        return

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
