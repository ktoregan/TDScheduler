import aiohttp
import asyncio
import os
import logging
from datetime import datetime, date
import pytz
from database import get_db_connection
import traceback

logging.basicConfig(
    filename="logs/fetch_schedule.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Timezone definitions
eastern = pytz.timezone("America/New_York")
irish = pytz.timezone("Europe/Dublin")

# Week mapping with start dates for each week in the 2024 season
week_mapping = {
    7: date(2024, 10, 17),
    8: date(2024, 10, 24),
    9: date(2024, 10, 31),
    10: date(2024, 11, 7),
    11: date(2024, 11, 14),
    12: date(2024, 11, 21),
    13: date(2024, 11, 28),
    14: date(2024, 12, 5),
    15: date(2024, 12, 12),
    16: date(2024, 12, 19),
    17: date(2024, 12, 26),
    18: date(2025, 1, 2)
}

# Function to convert ET game time to Irish time
def convert_to_irish_time(game_date, game_time_et):
    if game_time_et == "TBD" or not game_time_et:
        return None
    try:
        game_time_str = game_time_et.replace("a", "AM").replace("p", "PM")
        game_time_obj = datetime.strptime(game_time_str, "%I:%M%p")
        game_datetime = datetime.strptime(game_date, "%Y%m%d").replace(
            hour=game_time_obj.hour, minute=game_time_obj.minute, second=0, microsecond=0, tzinfo=eastern
        )
        return game_datetime.astimezone(irish).replace(second=0, microsecond=0)
    except Exception as e:
        logging.error(f"Error converting game time: {game_time_et}. Error: {e}")
        return None

# Determine the week number based on game date
def determine_week(game_date):
    game_date_obj = datetime.strptime(game_date, "%Y%m%d").date()
    sorted_weeks = sorted(week_mapping.items())
    for i, (week, start_date) in enumerate(sorted_weeks):
        if i == len(sorted_weeks) - 1 or (start_date <= game_date_obj < sorted_weeks[i + 1][1]):
            return week
    logging.warning(f"Could not determine week for game date: {game_date}")
    return None

# Asynchronous function to fetch game data from the API
async def fetch_game_data():
    url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLGamesForWeek"
    headers = {
        "x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
        "x-rapidapi-host": "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
    }
    params = {"week": "all", "seasonType": "reg", "season": "2024"}

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=headers, params=params) as response:
                if response.status == 200:
                    logging.info("Successfully fetched game schedule from API")
                    response_data = await response.json()
                    return response_data.get('body', []), 1
                else:
                    error_text = await response.text()
                    logging.error(f"Failed to fetch game schedule: {response.status} - {error_text}")
                    return None, 0
        except aiohttp.ClientError as e:
            logging.error(f"Error fetching game schedule from API: {e}")
            return None, 0

# Function to update API usage
def update_api_usage(api_calls, db, cursor):
    current_month_year = datetime.now().strftime("%Y-%m")

    # Check the current usage for the month
    cursor.execute("SELECT request_count FROM api_usage WHERE month_year = %s", (current_month_year,))
    result = cursor.fetchone()

    if result:
        new_count = result[0] + api_calls
        cursor.execute("UPDATE api_usage SET request_count = %s, request_time = NOW() WHERE month_year = %s",
                       (new_count, current_month_year))
        logging.info(f"Updated API usage for month {current_month_year}: new count is {new_count}")
    else:
        cursor.execute("INSERT INTO api_usage (request_count, request_time, month_year) VALUES (%s, NOW(), %s)",
                       (api_calls, current_month_year))
        logging.info(f"Inserted new API usage record for month {current_month_year}: count is {api_calls}")

# Function to update pick_window table
def update_pick_window_table(games, week, season, db, cursor):
    game_times = [
        (convert_to_irish_time(game["gameDate"], game["gameTime"]), game["gameDate"])
        for game in games if game["gameWeek"] == f"Week {week}"
    ]

    if not game_times:
        logging.warning(f"No valid game times for week {week}.")
        return

    for start_time, game_date in game_times:
        if start_time is None:
            continue

        day_name = start_time.strftime("%A")
        cursor.execute(
            """
            SELECT id FROM pick_window 
            WHERE week = %s AND season = %s AND start_time = %s
            """,
            (week, season, start_time)
        )
        existing_record = cursor.fetchone()

        if not existing_record:
            cursor.execute(
                """
                INSERT INTO pick_window (week, season, day_name, start_time, is_open, last_updated)
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """,
                (week, season, day_name, start_time, 0)
            )
            logging.info(f"Inserted new pick window entry for week {week}, season {season}, start time {start_time}")
        else:
            logging.debug(f"Duplicate entry skipped for week {week}, season {season}, start time {start_time}")

# Function to upsert game data into the games table
def upsert_game_data(games, db, cursor):
    for game in games:
        game_id = game.get("gameID")
        season_type = game.get("seasonType")
        week = determine_week(game.get("gameDate"))
        if week is None:
            logging.warning(f"Skipping game {game_id} due to undefined week.")
            continue

        home_team = game.get("home")
        away_team = game.get("away")
        teamID_home = game.get("teamIDHome")
        teamID_away = game.get("teamIDAway")
        game_status = game.get("gameStatus")
        game_status_code = int(game.get("gameStatusCode", 0))
        neutral_site = 1 if game.get("neutralSite") == "True" else 0
        espn_link = game.get("espnLink")
        cbs_link = game.get("cbsLink")
        season = game.get("season")
        game_time = convert_to_irish_time(game.get("gameDate"), game.get("gameTime"))

        # Perform update or insert
        try:
            cursor.execute(
                """
                INSERT INTO games (game_id, season_type, week, home_team, away_team, teamID_home, teamID_away, game_time,
                                   game_status, game_status_code, neutral_site, espn_link, cbs_link, last_updated, season)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s)
                ON DUPLICATE KEY UPDATE
                    season_type = VALUES(season_type),
                    week = VALUES(week),
                    home_team = VALUES(home_team),
                    away_team = VALUES(away_team),
                    teamID_home = VALUES(teamID_home),
                    teamID_away = VALUES(teamID_away),
                    game_time = VALUES(game_time),
                    game_status = VALUES(game_status),
                    game_status_code = VALUES(game_status_code),
                    neutral_site = VALUES(neutral_site),
                    espn_link = VALUES(espn_link),
                    cbs_link = VALUES(cbs_link),
                    last_updated = CURRENT_TIMESTAMP,
                    season = VALUES(season)
                """,
                (game_id, season_type, week, home_team, away_team, teamID_home, teamID_away, game_time,
                 game_status, game_status_code, neutral_site, espn_link, cbs_link, season)
            )
            logging.info(f"Upsert query executed for game_id: {game_id}")
        except Exception as e:
            logging.error(f"Error executing upsert for game_id {game_id}: {e}")
            logging.error(traceback.format_exc())

# Main execution with asyncio
async def main():
    logging.info("Starting TD Showdown game schedule update.")
    db = get_db_connection()
    cursor = db.cursor()
    try:
        games, api_calls = await fetch_game_data()
        season = 2024  # Define season

        if games:
            logging.info(f"Fetched {len(games)} games from the API.")
            upsert_game_data(games, db, cursor)  # Call to upsert game data directly after fetching games.
            for week in range(7, 19):  # Only weeks from 7 to 18 as per the given mapping.
                update_pick_window_table(games, week, season, db, cursor)

        # Update API usage here with the correct arguments
        update_api_usage(api_calls, db, cursor)
        logging.info("Game schedule update completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during the schedule update: {e}")
        logging.error(traceback.format_exc())
    finally:
        cursor.close()
        db.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    asyncio.run(main())