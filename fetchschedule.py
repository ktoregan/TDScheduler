import requests
import os
import logging
from datetime import datetime, date
import pytz
from database import get_db_connection

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

# Function to fetch game data from the API
def fetch_game_data():
    url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLGamesForWeek"
    headers = {
        "x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
        "x-rapidapi-host": "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
    }
    try:
        response = requests.get(url, headers=headers, params={"week": "all", "seasonType": "reg", "season": "2024"})
        response.raise_for_status()
        logging.info("Successfully fetched game schedule from API")
        update_api_usage(1)
        return response.json().get('body', []), 1
    except requests.RequestException as e:
        logging.error(f"Error fetching game schedule from API: {e}")
        return None, 0

# Function to update API usage
def update_api_usage(api_calls):
    current_month_year = datetime.now().strftime("%Y-%m")

    db = get_db_connection()
    cursor = db.cursor()

    cursor.execute("SELECT request_count FROM api_usage WHERE month_year = %s", (current_month_year,))
    result = cursor.fetchone()

    if result:
        new_count = result[0] + api_calls
        cursor.execute("UPDATE api_usage SET request_count = %s, request_time = NOW() WHERE month_year = %s",
                       (new_count, current_month_year))
    else:
        cursor.execute("INSERT INTO api_usage (request_count, request_time, month_year) VALUES (%s, NOW(), %s)",
                       (api_calls, current_month_year))

    db.commit()
    cursor.close()
    db.close()
    logging.info(f"API usage updated. Total requests this month: {new_count if result else api_calls}.")

# Function to update pick_window table
def update_pick_window_table(games, week, season):
    db = get_db_connection()
    cursor = db.cursor()

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
            logging.info(f"Duplicate entry skipped for week {week}, season {season}, start time {start_time}")

    db.commit()
    cursor.close()
    db.close()
    logging.info(f"Pick window table update completed for week {week}, season {season}.")

# Function to upsert game data into the games table
# Function to upsert game data into the games table
def upsert_game_data(games):
    db = get_db_connection()
    cursor = db.cursor()

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

        # Check if game already exists and update only if data has changed
        cursor.execute("SELECT * FROM games WHERE game_id = %s", (game_id,))
        existing_game = cursor.fetchone()

        # If game exists, check if data needs updating
        if existing_game:
            # Check if each column has the same value; if any differ, update is needed
            existing_values = {
                'season_type': existing_game[1],
                'week': existing_game[2],
                'home_team': existing_game[3],
                'away_team': existing_game[4],
                'teamID_home': existing_game[5],
                'teamID_away': existing_game[6],
                'game_time': existing_game[7],
                'game_status': existing_game[8],
                'game_status_code': existing_game[9],
                'neutral_site': existing_game[10],
                'espn_link': existing_game[11],
                'cbs_link': existing_game[12],
                'season': existing_game[14]
            }

            new_values = {
                'season_type': season_type,
                'week': week,
                'home_team': home_team,
                'away_team': away_team,
                'teamID_home': teamID_home,
                'teamID_away': teamID_away,
                'game_time': game_time,
                'game_status': game_status,
                'game_status_code': game_status_code,
                'neutral_site': neutral_site,
                'espn_link': espn_link,
                'cbs_link': cbs_link,
                'season': season
            }

            if existing_values != new_values:
                cursor.execute(
                    """
                    UPDATE games SET
                        season_type = %s,
                        week = %s,
                        home_team = %s,
                        away_team = %s,
                        teamID_home = %s,
                        teamID_away = %s,
                        game_time = %s,
                        game_status = %s,
                        game_status_code = %s,
                        neutral_site = %s,
                        espn_link = %s,
                        cbs_link = %s,
                        last_updated = CURRENT_TIMESTAMP,
                        season = %s
                    WHERE game_id = %s
                    """,
                    (season_type, week, home_team, away_team, teamID_home, teamID_away,
                     game_time, game_status, game_status_code, neutral_site, espn_link, cbs_link, season, game_id)
                )
                logging.info(f"Updated game data for game_id: {game_id}")

        else:
            # Insert if game does not exist
            cursor.execute(
                """
                INSERT INTO games (game_id, season_type, week, home_team, away_team, teamID_home, teamID_away, game_time,
                                   game_status, game_status_code, neutral_site, espn_link, cbs_link, last_updated, season)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s)
                """,
                (game_id, season_type, week, home_team, away_team, teamID_home, teamID_away, game_time,
                 game_status, game_status_code, neutral_site, espn_link, cbs_link, season)
            )
            logging.info(f"Inserted new game data for game_id: {game_id}")

    db.commit()
    cursor.close()
    db.close()
    logging.info("Games table update completed successfully.")

if __name__ == "__main__":
    logging.info("Starting TD Showdown game schedule update.")
    games, api_calls = fetch_game_data()
    season = 2024  # Define season

    if games:
        for week in range(1, 19):  # Example for 18 weeks
            update_pick_window_table(games, week, season)

    update_api_usage(api_calls)
    logging.info("Game schedule update completed successfully.")