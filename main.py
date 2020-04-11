from typing import Any
from typing import List
from typing import Tuple

from datetime import date, timedelta
import pandas as pd
import configparser
import click
import psycopg2
import requests

from helpers import format_data, create_tables, drop_tables, insert_to_table, \
    generate_insight, get_insight_3_df, generate_heatmap
from sql_queries import commits_table_insert, authors_table_insert, insight_1_query, \
    insight_2_query, insight_3_query


def get_data(organization: str, repository: str, start_date: str, end_date: str) -> Tuple[List[Tuple[Any]], List[Tuple[Any]]]:
    """
    Retrieves data to insert into database.
    ------------
    Commits are returned in a list format.
    index 0: latest commit
    index -1: earliest commit
    """

    commits_info = []
    firstlast_dates_info = []

    all_responses = []
    try:
        endpoint = f"https://api.github.com/repos/{organization}/{repository}/commits"

        # Restricting to maximum of 1000 commits
        for i in range(1, 11):
            payload = {
                'since': start_date,
                'until': end_date,
                'per_page': 100,
                'page': i
            }
            response = requests.get(endpoint, params=payload)  # type: ignore
            response_data = response.json()
            if response_data:
                all_responses += response_data
            else:
                # No more further pages
                break
        print(f'Length of data: {len(all_responses)}')

        for commit in range(len(all_responses)):
            commit_info, firstlast_date_info = format_data(commit, all_responses)
            commits_info.append(commit_info)
            if firstlast_date_info not in firstlast_dates_info:
                # Filter out duplicates here; setting UNIQUE on column would increase write time due to btree search
                firstlast_dates_info.append(firstlast_date_info)
    except IndexError:
        print("No commits found in specified time period. Please use an earlier start date.")
    except Exception as e:
        print(f"Exception found: {e}")
        raise
    return commits_info, firstlast_dates_info


@click.command()
@click.option('--organization', default="apache")
@click.option('--repository', default="airflow")
@click.option('--start_date', default=(date.today() - timedelta(hours=24)).strftime('%Y-%m-%dT%H:%M:%S'), type=click.DateTime(formats=["%Y-%m-%dT%H:%M:%S"]))
@click.option('--end_date', default=(date.today() + timedelta(hours=24)).strftime('%Y-%m-%dT%H:%M:%S'), type=click.DateTime(formats=["%Y-%m-%dT%H:%M:%S"]))
def main(organization: str, repository: str, start_date: str, end_date: str) -> None:
    """
    end_date takes in additional timedelta(hours = 24), since we're interested in end_date's results as well (inclusively).
    """
    if start_date > end_date:
        raise ValueError("Start date needs to be earlier than End date")

    config = configparser.ConfigParser()
    config.read('configs.cfg')

    commits_info, authors_info = get_data(organization, repository, start_date, end_date)

    # Connect to Postgres
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['POSTGRES'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    print(f"Commits count: {len(commits_info)}")
    insert_to_table(cur, conn, authors_table_insert, authors_info)  # type: ignore
    insert_to_table(cur, conn, commits_table_insert, commits_info)  # type: ignore

    insight_1_results = generate_insight(cur, conn, insight_1_query)
    print(f'Insight 1 (Top 3 Authors):\n \
        1st: {insight_1_results[0][1]}({insight_1_results[0][0]}) -> {insight_1_results[0][2]} commits\n \
        2nd: {insight_1_results[1][1]}({insight_1_results[1][0]}) -> {insight_1_results[1][2]} commits\n \
        3rd: {insight_1_results[2][1]}({insight_1_results[2][0]}) -> {insight_1_results[2][2]} commits\n')

    insight_2_results = generate_insight(cur, conn, insight_2_query)
    print(f'Insight 2 (Author with longest contribution window):\n \
        {insight_2_results[0][0]} -> {insight_2_results[0][1]} hours\n')

    insight_3_results = generate_insight(cur, conn, insight_3_query)
    insight_3_temp_df = pd.DataFrame(insight_3_results, columns=['date'])
    print(f'Insight 3 (Heatmap of Github commits): Heatmap image generate to /output directory.')

    insight_3_df = get_insight_3_df(insight_3_temp_df)
    insight_3_df_pivot = insight_3_df.pivot("day", "time_interval", "total_commits")

    # Generate heatmap
    generate_heatmap(insight_3_df_pivot, organization, repository)


if __name__ == "__main__":
    main()
