"""
Updates leagues meta csv.
"""

from typing import Dict, List
import json
import os
import logging

import requests
import boto3
import botocore
import pandas as pd

RAPID_API_KEY = os.environ.get("RAPID_API_KEY")
SEASON_META_TABLE = os.environ.get("SEASONS_META_TABLE")
FIRST_YEAR = 2019

dynamodb = boto3.resource("dynamodb")
SEASON_META_TABLE = dynamodb.Table(SEASON_META_TABLE)


def get_seasons_data_from_api() -> List[Dict[str, str]]:
    """
    Retrieves the seasons data from the API.

    Returns:
        A list of dictionaries representing the seasons data.

    Raises:
        Exception: If the API request fails or returns a non-200 status code.
    """
    headers = {
        "x-rapidapi-host": "v3.football.api-sports.io",
        "x-rapidapi-key": RAPID_API_KEY,
    }

    session = requests.Session()
    site = "https://v3.football.api-sports.io/"

    response = session.get(site + "leagues", headers=headers)

    if response.status_code == 200:
        return response.json()["response"]
    else:
        raise Exception(f"Status code: {response.status_code}")


def process_json_to_dataframe(data: Dict, first_year_kept: int) -> pd.DataFrame:
    """
    Process a dictionary and convert it to a pandas DataFrame.

    Args:
        data (Dict): The input dictionary containing data to be processed.
        first_year_kept (int): The first year to keep in the DataFrame.

    Returns:
        pd.DataFrame: The processed DataFrame containing the desired information.
            with columns:
                [
                'season_year', 'start_date', 'end_date', 'current',
                'coverage_fixtures_events', 'coverage_fixtures_lineups',
                'coverage_fixtures_statistics_fixtures',
                'coverage_fixtures_statistics_players', 'coverage_standings',
                'coverage_players', 'coverage_top_scorers', 'coverage_top_assists',
                'coverage_top_cards', 'coverage_injuries', 'coverage_predictions',
                'coverage_odds', 'league_id', 'league_name', 'league_type',
                'league_logo', 'country_name', 'country_code', 'country_flag'
                ]
    """
    seasons = None
    seasons = pd.json_normalize(
        data,
        record_path=["seasons"],
        meta=[
            ["league", "id"],
            ["league", "name"],
            ["league", "type"],
            ["league", "logo"],
            ["country", "name"],
            ["country", "code"],
            ["country", "flag"],
        ],
    )

    seasons.columns = seasons.columns.str.replace(".", "_", regex=False)
    seasons = seasons.infer_objects()

    seasons = seasons[
        (seasons["league_type"] != "Cup")
        & (seasons["year"] > first_year_kept)
        & (seasons["coverage_fixtures_events"])
    ]
    seasons = seasons.rename(
        columns={"year": "season_year", "start": "start_date", "end": "end_date"}
    )

    # Drop duplicates as we are only interested on the latest information
    seasons = seasons.drop_duplicates(subset=["league_id", "season_year"], keep="last")
    return seasons


def put_item(table, item: Dict, condition_expression: str) -> None:
    """
    Puts an item into a table with a condition expression.

    Parameters:
    - table: Dynamodb resource of the table.
    - item: A dictionary representing the item to be put into the table.
    - condition_expression: A string representing the condition expression for the put operation.

    Returns:
        None

    Raises:
    - botocore.exceptions.ClientError: If the put operation fails due to a client error, except for a ConditionalCheckFailedException.

    Todo:
    - If we fail to insert one item the whole batch / application should not fail
    """
    try:
        table.put_item(Item=item, ConditionExpression=condition_expression)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise


def lambda_handler(event, context):
    """
    Fetches the meta information from all seasons from the RAPID-API and stores them to
    AWS DynamoDB to be processed later.
    """
    new_data = process_json_to_dataframe(
        get_seasons_data_from_api(), first_year_kept=FIRST_YEAR
    )
    logging.debug(new_data.columns)
    logging.debug(new_data)

    # Add placeholder columns to dataframe. If it does not yet exist with our database
    # we can set the lastly updated fixtures to some time in past set that we need
    # to pull games from that season
    new_data["last_updated_fixtures"] = "2000-01-01"
    new_data["data_need_to_update"] = 1

    for item in new_data.to_dict("records"):
        logging.debug(item)
        # Put item to database. If it already exists (we are already tracking the collected matches)
        # we don't need to put it in. The condition expression checks for this
        put_item(
            table=SEASON_META_TABLE,
            item=item,
            condition_expression="attribute_not_exists(league_id) and attribute_not_exists(season_year)",
        )

    return {"statusCode": 200, "body": "Meta information succesfully imported"}


if __name__ == "__main__":
    # For testing this function fast locally without starting a stack
    lambda_handler(None, None)
