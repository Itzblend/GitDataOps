from datetime import datetime, timedelta
from typing import List
from scipy.stats import rankdata
from colorama import Fore


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


def print_colored_number(message, number):
    if number < 1 or number > 5:
        print(f"Number {number}")
        return

    # Calculate the color code based on the number
    green_code = 255
    red_code = min(int(255 - (number - 1) * 51), 150)

    # Generate the ANSI escape code for the color
    color = f"\033[38;2;{red_code};{green_code};0m"

    # Reset the color at the end of the line
    reset = Fore.RESET

    # Print the colored number
    return f"{color}{message}{reset}"
