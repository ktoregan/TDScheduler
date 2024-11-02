import aiohttp
import asyncio
import mysql.connector
import os
from dotenv import load_dotenv
import logging
import datetime
from database import get_db_connection

# Load environment variables
load_dotenv()

# Logging setup
logging.basicConfig(
    filename="logs/get_player_info.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Function to ensure that environment variables are set properly
def get_env_var(var_name):
    value = os.getenv(var_name)
    if not value:
        logging.error(f"Environment variable '{var_name}' is not set.")
        raise ValueError(f"Environment variable '{var_name}' is required.")
    return value

# Asynchronous function to fetch team data
async def fetch_team_data():
    url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLTeams"
    headers = {
        "x-rapidapi-key": get_env_var("RAPIDAPI_KEY"),
        "x-rapidapi-host": get_env_var("RAPIDAPI_HOST")
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                logging.info("Successfully fetched team data from API.")
                update_api_usage(1)
                json_response = await response.json()
                return json_response.get("body", [])
            else:
                error_text = await response.text()
                logging.error(f"Failed to fetch team data: {response.status} {error_text}")
                return None

# Asynchronous function to fetch player data
async def fetch_player_list():
    url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLPlayerList"
    headers = {
        "x-rapidapi-key": get_env_var("RAPIDAPI_KEY"),
        "x-rapidapi-host": get_env_var("RAPIDAPI_HOST")
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                logging.info("Successfully fetched player list from API")
                update_api_usage(1)
                json_response = await response.json()
                return json_response.get("body", [])
            else:
                error_text = await response.text()
                logging.error(f"Failed to fetch player list: {response.status} {error_text}")
                return None

# Function to update API usage count
def update_api_usage(api_calls):
    current_month_year = datetime.datetime.now().strftime('%Y-%m')

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

# Function to notify users about injured players
def notify_injured_players(cursor, player_id, injury_status):
    try:
        cursor.execute("SELECT user_id, week FROM picks WHERE player_id = %s AND is_successful = 0 AND Is_injured = 0", (player_id,))
        picks = cursor.fetchall()

        if picks:
            for pick in picks:
                user_id = pick['user_id']
                week = pick['week']

                cursor.execute("UPDATE picks SET Is_injured = 1 WHERE player_id = %s AND user_id = %s AND week = %s", 
                               (player_id, user_id, week))
                logging.info(f"Player {player_id} is injured. Marked pick for user {user_id} for week {week} as injured. Notifying user.")
                # Send notification to the user (bot integration could be placed here)

    except mysql.connector.Error as err:
        logging.error(f"Failed to notify users for injured player {player_id}: {err}")

# Function to update player information and bye weeks
async def upsert_player_info():
    players = await fetch_player_list()
    if not players:
        logging.error("No player data to process.")
        return

    teams = await fetch_team_data()
    if not teams:
        logging.error("Team data not available. Exiting.")
        return

    try:
        db = get_db_connection()
        cursor = db.cursor(dictionary=True)

        for player in players:
            player_id = player.get("playerID")
            player_name = player.get("longName")
            team_name = player.get("team")
            team_id = player.get("teamID")
            position = player.get("pos")
            is_free_agent = 1 if player.get("isFreeAgent", "False") == "True" else 0
            injury_status = player["injury"].get("designation", "Healthy")
            headshot_url = player.get("espnHeadshot")

            cursor.execute("SELECT * FROM players WHERE player_id = %s", (player_id,))
            existing_player = cursor.fetchone()

            if existing_player:
                if existing_player['team_name'] != team_name:
                    logging.info(f"Player {player_name} changed teams from {existing_player['team_name']} to {team_name}. Updating bye week...")
                    matching_team = next((team for team in teams if team["teamAbv"] == team_name), None)
                    if matching_team:
                        byeweek = matching_team.get("byeWeeks", {}).get("2024", [None])[0]
                        if byeweek:
                            cursor.execute("UPDATE players SET byeweek = %s, last_updated = CURRENT_TIMESTAMP WHERE player_id = %s", 
                                           (byeweek, player_id))
                            logging.info(f"Updated bye week for player_id {player_id} to week {byeweek}")

                if injury_status in ["Doubtful", "Out", "Injured Reserve"]:
                    logging.info(f"Player {player_name} is injured with status {injury_status}. Notifying users...")
                    notify_injured_players(cursor, player_id, injury_status)

                cursor.execute("""
                    UPDATE players
                    SET player_name = %s, team_name = %s, team_id = %s, position = %s, is_free_agent = %s, injury_status = %s, headshot_url = %s, last_updated = CURRENT_TIMESTAMP
                    WHERE player_id = %s
                """, (player_name, team_name, team_id, position, is_free_agent, injury_status, headshot_url, player_id))

            else:
                cursor.execute("""
                    INSERT INTO players (player_id, player_name, team_name, team_id, position, is_free_agent, injury_status, headshot_url, last_updated)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """, (player_id, player_name, team_name, team_id, position, is_free_agent, injury_status, headshot_url))

        db.commit()
        logging.info("Player information and injury update completed.")

    except mysql.connector.Error as err:
        logging.error(f"Database operation failed: {err}")
    finally:
        cursor.close()
        db.close()

# Main execution
if __name__ == "__main__":
    logging.info("Starting player info, injury check, and bye week update.")
    asyncio.run(upsert_player_info())
    logging.info("Player info, injury check, and bye week update completed successfully.")