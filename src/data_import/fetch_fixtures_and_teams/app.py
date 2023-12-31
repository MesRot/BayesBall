"""
Module for updating the teams and fixtures for football leagues.
Does not update all the league / season combinations at once to stay under API-limits
The amount of league / season combinations updated is controlled by setting different values
for MAX_UPDATES environment variable.

Improvements:
TODO: Make the leagues we need to update in main lambda_handler a pydantic object instead of dictionary to make following the code easier.
"""

import json
from typing import Optional, Dict, List
import os
import logging
import time
from datetime import datetime
from io import StringIO
import sys

import requests
import pandas as pd
import boto3

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

RAPID_API_KEY = os.environ.get("RAPID_API_KEY")
DESTINATION_BUCKET = os.environ.get("DESTINATION_BUCKET")
MAX_UPDATES = int(os.environ.get("MAX_UPDATES", 1))
SLEEP_TIME_BETWEEN_CALLS_SECONDS = int(
    os.environ.get("SLEEP_TIME_BETWEEN_CALLS_SECONDS", 5)
)

SEASON_META_TABLE = os.environ.get("SEASONS_META_TABLE")

dynamodb = boto3.resource("dynamodb")
SEASON_META_TABLE = dynamodb.Table(SEASON_META_TABLE)

s3_resource = boto3.resource("s3")


def upload_df_to_s3(bucket_name: str, key: str, df: pd.DataFrame) -> None:
    """
    Uploads a DataFrame to an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        key (str): The key (path) where the DataFrame will be stored in the bucket.
        df (pd.DataFrame): The DataFrame to be uploaded.

    Returns:
        None
    """
    # Convert dataframe to text file in memory to be able to write it to S3
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, sep=";")

    # Upload the text file from memory to S3
    s3_resource.Object(bucket_name, key).put(Body=csv_buffer.getvalue())


def get_data(league_id: int, season: int, endpoint: str) -> Optional[List[Dict]]:
    """
    Retrieves data from the API for a specific league, season, and endpoint.

    Args:
        league_id: The ID of the league.
        season: The year of the season.
        endpoint: The API endpoint to retrieve data from.
            Valid values are 'fixtures' or 'teams'.

    Returns:
        A dictionary containing the response data from the API.
        If no data is found or an error occurs, will return None.
    """
    headers = {
        "x-rapidapi-host": "v3.football.api-sports.io",
        "x-rapidapi-key": RAPID_API_KEY,
    }
    session = requests.Session()
    site = "https://v3.football.api-sports.io/"

    params = {"league": int(league_id), "season": int(season)}

    # warning: this logs the API-key, TODO: fix later to mask the API-key.
    logging.debug(headers, params, endpoint)

    response = session.get(site + endpoint, headers=headers, params=params)

    logging.debug(response.text)
    logging.debug(response.json())

    data = None
    if response.status_code == 200:
        if len(response.json()["response"]) > 0:
            data = response.json()["response"]
        else:
            logging.info(
                f"No {endpoint} found for league_id={league_id} season={season}"
            )
            logging.info(response.json()["response"])
    else:
        logging.info(f"Error, API responded with: {response.status_code}")
    return data


def process_fixtures_to_df(fixtures_json: dict) -> pd.DataFrame:
    """
    Process the fixtures data from a JSON object and convert it into a pandas DataFrame.

    Parameters:
        fixtures_json (dict): A dictionary containing the fixtures data in JSON format.

    Returns:
        pd.DataFrame: A DataFrame containing the processed fixtures data.

    """
    df = pd.json_normalize(fixtures_json)
    df.columns = (
        df.columns.str.replace(".", "_", regex=False)
        .str.replace("fixture", "game")
        .str.replace("halftime", "ht")
        .str.replace("fulltime", "ft")
    )
    df = df[
        [
            "game_id",
            "game_date",
            "league_id",
            "league_name",
            "league_season",
            "teams_home_id",
            "teams_home_name",
            "teams_away_id",
            "teams_away_name",
            "score_ht_home",
            "score_ft_home",
            "score_ht_away",
            "score_ft_away",
        ]
    ]
    df["game_date"] = pd.to_datetime(df["game_date"])

    return df


def process_teams_to_df(teams_json: dict) -> pd.DataFrame:
    """
    Converts a JSON object containing teams data into a pandas DataFrame.

    Parameters:
    teams_json (dict): A dictionary containing teams data in JSON format.

    Returns:
    pd.DataFrame: A DataFrame containing the processed teams data.
        with columns:
            ["team_id", "team_name", "team_code", "team_logo", "team_country"]
    """
    # Convert json to dataframe
    df = pd.json_normalize(teams_json)
    df.columns = df.columns.str.replace(".", "_", regex=False)

    # Select only the columns we need
    df = df[["team_id", "team_name", "team_code", "team_logo", "team_country"]]

    return df


def get_leagues_to_update(table, limit: int = MAX_UPDATES):
    """
    Fetches leagues to update from DynamoDB.

    Args:
        limit (int): The maximum number of items to retrieve. Defaults to MAX_UPDATES.
        table (dynamodb resource): The dynamodb table where to check new leagues to update

    Returns:
        List[dict]: A list of dictionaries containing the retrieved items.

    Raises:
        KeyError: If the 'Items' key is not present in the response.

    Description:
        This function fetches leagues to update from the DynamoDB table 'BayesballSeasonsMeta'.
        The table has a global secondary index with a hash column 'data_need_to_update' and a range
        column 'last_updated_fixtures'.

        The function queries the table for items that need updating (data_need_to_update = 1) and
        retrieves the least recently updated 5 items. The items are ordered by the 'last_updated_fixtures'
        column in descending order.

    Example Usage:
        leagues = get_leagues_to_update(10)
        for league in leagues:
            print(league)
    """
    response = table.query(
        IndexName="LastUpdatedIndex",
        KeyConditionExpression="data_need_to_update = :val",
        ExpressionAttributeValues={":val": 1},
        ScanIndexForward=False,
        Limit=limit,
    )
    return response["Items"]


def fetch_and_upload_teams(league_id: int, season_year: int):
    """
    Fetches teams from the API and uploads them to S3.

    Args:
        league_id (int): The ID of the league.
        season_year (int): The year of the season.

    Returns:
        None
    """
    teams = get_data(league_id, season_year, endpoint="teams")
    teams = process_teams_to_df(teams)
    upload_df_to_s3(
        DESTINATION_BUCKET, f"teams/{league_id}/{season_year}/data.csv", df=teams
    )


def update_the_dymamodb_meta_table(
    table,
    league_id: int,
    season_year: int,
    last_updated_teams: Optional[str],
    last_updated_fixtures: str,
    newest_game_date: str,
    posteriors_need_to_update: bool,
    data_need_to_update: bool,
):
    """
    Updates the dynamodb meta table.
    We need to do:
        - remove the "data_need_to_update" value from the item if the todays date is later than the end_date
        - update posteriors_need_to_update value to 1 if the model_last_updated_date value is higher than the last_game_date
        - update the "last_updated_teams" and "last_updated_fixtures" to current date
    """

    update_expression = """
    set
        last_updated_fixtures = :last_updated_fixtures,
        newest_game_date = :newest_game_date
    """

    new_values = {
        ":last_updated_fixtures": last_updated_fixtures,
        ":newest_game_date": newest_game_date,
    }

    if last_updated_teams is not None:
        update_expression += ",\n\tlast_updated_teams = :last_updated_teams"
        new_values[":last_updated_teams"] = last_updated_teams

    if posteriors_need_to_update:
        update_expression += (
            ",\n\tposteriors_need_to_update = :posteriors_need_to_update"
        )
        new_values[":posteriors_need_to_update"] = 1

    if data_need_to_update is False:
        update_expression += "\n\t REMOVE data_need_to_update"

    logging.debug(update_expression)

    return table.update_item(
        Key={"league_id": league_id, "season_year": season_year},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=new_values,
        ReturnValues="UPDATED_NEW",
    )


def get_newest_game_date(fixture_df: pd.DataFrame) -> str:
    """
    Returns the date of the most recent game in the fixture dataframe.

    Parameters:
    fixture_df (pd.DataFrame): A pandas DataFrame containing the game fixture data.

    Returns:
    str: The date of the most recent game in the fixture dataframe.
    """
    return (
        fixture_df.assign(day=fixture_df["game_date"].dt.strftime("%Y-%m-%d"))[
            ["day", "score_ft_home"]
        ]
        .groupby("day")
        .agg(  # There is future games which does not yet have scores
            lambda x: not x.isna().any()
        )
        .reset_index()
        .query("score_ft_home")["day"]
        .max()
    )


def lambda_handler(event, context):
    """
    Main function for updating the team and fixture information for
    teams and leagues.

    """
    todays_date = datetime.now().strftime("%Y-%m-%d")

    leagues_we_need_to_update = get_leagues_to_update(table=SEASON_META_TABLE)
    logging.info(f"Leagues we need to update: {leagues_we_need_to_update}")

    for league_which_needs_updating in leagues_we_need_to_update:
        season_year = int(league_which_needs_updating["season_year"])
        league_id = int(league_which_needs_updating["league_id"])

        start_date = league_which_needs_updating["start_date"]
        end_date = league_which_needs_updating["end_date"]
        last_updated_posteriors = league_which_needs_updating.get(
            "last_updated_posteriors", "2000-01-01"
        )

        logging.info(f"Pulling data, league_id={league_id} & season_year={season_year}")
        logging.info(f"Last updated posteriors: {last_updated_posteriors}")

        last_updated_teams = league_which_needs_updating.get("last_updated_teams")
        teams_updated = False

        # update teams if we have not updated them after the season has started
        if last_updated_teams is None:
            fetch_and_upload_teams(league_id, season_year)
            teams_updated = True
        elif last_updated_teams < start_date:
            fetch_and_upload_teams(league_id, season_year)
            teams_updated = True

        # update fixtures
        fixtures = get_data(league_id, season_year, endpoint="fixtures")
        fixtures = process_fixtures_to_df(fixtures)

        upload_df_to_s3(
            DESTINATION_BUCKET,
            f"fixtures/{league_id}/{season_year}/data.csv",
            df=fixtures,
        )
        newest_game_date = get_newest_game_date(fixture_df=fixtures)
        data_need_to_update = end_date > todays_date
        logging.info(
            f"newest_game_date: {newest_game_date} with league_id={league_id} & season_year={season_year}"
        )

        update_the_dymamodb_meta_table(
            table=SEASON_META_TABLE,
            league_id=league_id,
            season_year=season_year,
            last_updated_teams=todays_date if teams_updated else None,
            last_updated_fixtures=todays_date,
            newest_game_date=newest_game_date,
            posteriors_need_to_update=last_updated_posteriors < newest_game_date,
            data_need_to_update=data_need_to_update,
        )
        # TODO: Stop doing this on last item if you want to optimize
        time.sleep(SLEEP_TIME_BETWEEN_CALLS_SECONDS)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "leagues_updated": "\n".join(
                    [
                        f"League id: {int(i['league_id'])}, Season year: {int(i['season_year'])}"
                        for i in leagues_we_need_to_update
                    ]
                )
            }
        ),
    }


if __name__ == "__main__":
    # For testing this function fast locally without starting a stack
    lambda_handler(None, None)
