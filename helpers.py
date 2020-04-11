from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import Union

import os
import datetime as dt
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from itertools import product

from sql_queries import create_table_queries, drop_table_queries


def format_data(commit_idx: int, all_data: List[Dict[str, Union[str, Dict, List]]]) -> Tuple[Tuple[Any], Tuple[Any]]:
    """
    By default, generates 2 lists of data for insertion into `authors` and `commits` tables.
    """
    commit = all_data[commit_idx]['commit']

    commit_info = [commit['author']['email'], commit['author']['date']]  # type: ignore
    commit_info = commit_info
    firstlast_date_info = extract_firstlast_dates(all_data, commit['author']['email'], commit['author']['name'])  # type: ignore

    return tuple(commit_info), firstlast_date_info  # type: ignore


def format_time_interval(date: str) -> str:
    """
    Generates 3-hour time-interval tag for each commit.
    """
    datetime_obj = dt.time(dt.datetime.strptime(date, '%Y-%m-%dT%H:%M:%SZ').hour)

    start = dt.time(0)
    end = dt.time(3)
    time_interval = None
    while not time_interval:
        if start <= datetime_obj < end:
            start_prefix = 'am' if start.hour < 12 else 'pm'
            end_prefix = 'am' if end.hour < 12 else 'pm'
            start_hour = str(start.hour) if start.hour <= 12 else str(start.hour - 12)
            end_hour = str(end.hour) if end.hour <= 12 else str(end.hour - 12)

            if start_hour == '0':
                start_hour = '12'
            time_interval = f"{start_hour}{start_prefix}-{end_hour}{end_prefix}"
            break
        elif (start.hour == 21) and (start <= datetime_obj):
            start_prefix = 'am' if start.hour < 12 else 'pm'
            end_prefix = 'am' if end.hour < 12 else 'pm'
            start_hour = str(start.hour) if start.hour < 12 else str(start.hour - 12)
            end_hour = '12'
            time_interval = f"{start_hour}{start_prefix}-{end_hour}{end_prefix}"
        else:
            start = dt.time(start.hour + 3)
            if end.hour + 3 > 23:
                end = dt.time(0)
            else:
                end = dt.time(end.hour + 3)

    return time_interval


def extract_firstlast_dates(all_data: List[Dict[str, Union[str, Dict, List]]], author_email: str, author_name: str) -> Tuple[Any]:
    """
    Generates timestamp for first and last commit of each author.
    """
    author_commits = [commit['commit']['author']['date'] for commit in all_data if commit['commit']['author']['email'] == author_email]  # type: ignore
    author_commit_dates = (author_email, author_name, min(author_commits), max(author_commits))

    return author_commit_dates  # type: ignore


def create_tables(cur: Any, conn: Any) -> None:
    """
    Run queries found in create_table_queries function from sql_queries.py.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def drop_tables(cur: Any, conn: Any) -> None:
    """
    Run queries found in drop_table_queries function from sql_queries.py.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def insert_to_table(cur: Any, conn: Any, query: str, data: List[Tuple]) -> None:
    """
    Run insertion queries found in sql_queries.py, based on given query.
    """
    cur.executemany(query, data)
    conn.commit()


def generate_insight(cur: Any, conn: Any, query: str) -> List[Tuple]:
    """
    Run insight queries found in sql_queries.py, based on given query.
    """
    cur.execute(query)
    rows = cur.fetchall()

    return rows


def get_skeleton_df() -> pd.DataFrame:
    """
    Generates main dataframe with every combination for heatmap.
    """
    all_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    all_time_intervals = ['12am-3am', '3am-6am', '6am-9am', '9am-12pm', '12pm-3pm', '3pm-6pm', '6pm-9pm', '9pm-12am']

    main_df = pd.DataFrame(list(product(all_days, all_time_intervals)), columns=['day', 'time_interval'])

    return main_df


def get_insight_3_df(temp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generates dataframe with NaN values replaced with 0 for heatmap.
    -------------------
    Reason for doing transformation here instead of loading it into Dataframe is because if the date is to be used internationally,
    storing a day of week or time_interval based on a specific timezone would be useless. Transformations based on timezones should
    be done independently when generating insights.
    """
    # Perform transformations here; timezone changes can be applied here
    temp_df['day'] = temp_df['date'].dt.day_name()
    temp_df['date_str'] = temp_df['date'].apply(lambda x: x.strftime('%Y-%m-%dT%H:%M:%SZ'))
    temp_df['time_interval'] = temp_df['date_str'].apply(lambda x: format_time_interval(x))
    temp_df.drop(columns=['date', 'date_str'], inplace=True)
    temp_df = temp_df.groupby(["day", "time_interval"]).size().reset_index(name="total_commits")

    main_df = get_skeleton_df()
    insight_3_df = pd.merge(main_df, temp_df, how='left', on=['day', 'time_interval'])

    # Values to map NaN values
    na_vals = {'total_commits': 0}
    insight_3_df.fillna(value=na_vals, inplace=True)

    # Convert dtype to int for seaborn heatmap
    insight_3_df['total_commits'] = insight_3_df['total_commits'].astype(int)

    # Sort by day column
    day_cats = pd.CategoricalDtype(categories=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], ordered=True)
    insight_3_df['day'] = insight_3_df['day'].astype(day_cats)

    # Sort by time_interval column
    time_interval_cats = pd.CategoricalDtype(categories=['12am-3am', '3am-6am', '6am-9am', '9am-12pm', '12pm-3pm', '3pm-6pm', '6pm-9pm', '9pm-12am'], ordered=True)
    insight_3_df['time_interval'] = insight_3_df['time_interval'].astype(time_interval_cats)

    return insight_3_df


def generate_heatmap(df_pivot: pd.DataFrame, organization: str, repository: str) -> None:
    """
    Generates seaborn heatmap and saves to /output directory.
    """
    plt.gcf().subplots_adjust(left=0.20, bottom=0.25)
    ax = sns.heatmap(df_pivot, annot=True, linewidths=.5, fmt="d")
    fig = ax.get_figure()
    fig.savefig(os.getcwd() + f"/output/{organization}_{repository}_heatmap.png")
