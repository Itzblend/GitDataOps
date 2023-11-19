from datetime import datetime, timedelta
from typing import List
from scipy.stats import rankdata
import os
from git_data_ops.src.logger import get_logger
import ruamel.yaml

logger = get_logger(__file__)


def dates_to_diff(datetime1: datetime, datetime2: datetime):
    if datetime2.date() == datetime.today().date():
        diff = datetime1 - datetime2
        if diff.seconds < 5 * 60:
            return "Just now"
        elif diff.seconds < 60 * 60:
            return "Past hour"
        else:
            return f"Today {diff.seconds / 60 / 60}"
    elif datetime2.date() == datetime.today().date() - timedelta(days=1):
        return "Yesterday"
    else:
        return f"{(datetime1 - datetime2).days} days ago"


def rank_by_date(dates: List[datetime], reverse: bool = False):  # (rank, idx)
    # Convert datetimes to timestamps for ranking
    timestamp_list = [date.timestamp() for date in dates]

    # Rank the timestamps, with equal values getting the same rank
    ranks = rankdata(timestamp_list, method="dense")

    # Combine the dates with their ranks
    ranked_dates = list(zip(dates, ranks))
    len_dates = len(ranked_dates)
    if reverse:
        return {date: len_dates - int(rank) + 1 for (date, rank) in ranked_dates}
    return {date: rank for (date, rank) in ranked_dates}


def init_project(project_root_dir: str = "./"):
    config_dir = os.path.join(project_root_dir, "config")
    if "config" in os.listdir():
        logger.error(f"Directory {config_dir} already exists")


def init_project(project_root_dir: str = "./"):
    config_dir = os.path.join(project_root_dir, "config")
    if "config" in os.listdir():
        logger.error("Directory 'config' already exists")
        return

    os.makedirs(config_dir, exist_ok=False)

    default_config = {
        "config_version": 1.0,
        "databases": {
            "default_database": {
                "type": "postgresql",
                "host": "localhost",
                "port": "5432",
                "username": "postgres",
                "password": "postgres",
                "database": "postgres",
            }
        },
        "database": "default_database",
    }

    write_yaml(os.path.join(config_dir, "database.yml"), default_config)


def write_yaml(file_path, data):
    with open(file_path, "w") as yaml_file:
        yaml = ruamel.yaml.YAML()
        yaml.dump(data, yaml_file)


def filter_dict_by_keys(pair, keys: List[str]) -> bool:
    key, value = pair
    if key in keys:
        return True  # keep pair in the filtered dictionary
    else:
        return False  # filter pair out of the dictionary
