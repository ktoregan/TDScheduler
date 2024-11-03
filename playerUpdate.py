import aiohttp
import asyncio
import os
import logging
from datetime import datetime
from database import get_db_connection
import traceback
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logging.basicConfig(
    filename="logs/get_player_info.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Function to get required environment variables
def get_env_var(var_name):
    value = os.getenv(var_name)
    if not value:
        raise ValueError(f"Environment variable {var_name} is missing.")
    return value

RAPIDAPI_KEY = get_env_var("RAPIDAPI_KEY")
RAPIDAPI_HOST = get_env_var("RAPIDAPI_HOST")

# Asynchronous function to fetch team data
async def fetch_team_data():
    url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLTeams"
    headers = {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": RAPIDAPI_HOST
    }

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    logging.info("Successfully fetched team data from API")
                    return await response.json()
                else:
                    error_text = await response.text()
                    logging.error(f"Failed to fetch team data: {response.status} - {error_text}")
                    return None
        except aiohttp.ClientError as e:
            logging.error(f"Error fetching team data from API: {e}")
            return None

# Asynchronous function to fetch player list
async def fetch_player_list():
    url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLPlayerList"
    headers = {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": RAPIDAPI_HOST
    }

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    logging.info("Successfully fetched player list from API")
                    response_data = await response.json()
                    return response_data.get('body', [])
                else:
                    error_text = await response.text()
                    logging.error(f"Failed to fetch player list: {response.status} - {error_text}")
                    return None
        except aiohttp.ClientError as e:
            logging.error(f"Error fetching player list from API: {e}")
            return None

# Function to update API usage count
def update_api_usage(api_calls):
    current_month_year = datetime.now().strftime('%Y-%m')

    db = get_db_connection()
    cursor = db.cursor()

    # Check the current usage for the month
    cursor.execute("SELECT request_count FROM api_usage WHERE month_year = %s", (current_month_year,))
    result = cursor.fetchone()

    if result:
        # Update the count by adding the new API calls
        new_count = result[0] + api_calls
        cursor.execute("UPDATE api_usage SET request_count = %s, request_time = NOW() WHERE month_year = %s", (new_count, current_month_year))
        logging.info(f"Updated API usage for month {current_month_year}: new count is {new_count}")
    else:
        # Insert a new row for the current month
        cursor.execute("INSERT INTO api_usage (month_year, request_count) VALUES (%s, %s)", (current_month_year, api_calls))
        logging.info(f"Inserted new API usage record for month {current_month_year}: count is {api_calls}")

    db.commit()
    cursor.close()
    db.close()

# Function to upsert player info into the players table
async def upsert_player_info():
    db = get_db_connection()
    cursor = db.cursor()

    try:
        # Fetch team and player data
        team_data = await fetch_team_data()
        players = await fetch_player_list()
        api_calls = 2  # One for team data and one for player data

        if not team_data or not players:
            logging.error("Failed to fetch data from the API.")
            return

        # Adjusted team mapping to match the correct key used in the API response
        team_mapping = {team['teamAbv']: team.get('byeWeeks', {}).get('2024', [None])[0] for team in team_data['body']}

        # Update player data in the database
        for player in players:
            player_id = player.get("playerID")
            player_name = player.get("longName")
            team_name = player.get("team")
            team_id = player.get("teamID")
            position = player.get("pos")
            is_free_agent = 1 if player.get("isFreeAgent", "False") == "True" else 0
            injury_status = player.get("injury", {}).get("designation", "Unknown")
            headshot_url = player.get("espnHeadshot")
            byeweek = team_mapping.get(team_name)

            # Upsert player information into the players table
            cursor.execute(
                """
                INSERT INTO players (player_id, player_name, team_name, team_id, position, is_free_agent, injury_status, headshot_url, last_updated, byeweek)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s)
                ON DUPLICATE KEY UPDATE
                    player_name = VALUES(player_name),
                    team_name = VALUES(team_name),
                    team_id = VALUES(team_id),
                    position = VALUES(position),
                    is_free_agent = VALUES(is_free_agent),
                    injury_status = VALUES(injury_status),
                    headshot_url = VALUES(headshot_url),
                    last_updated = CURRENT_TIMESTAMP,
                    byeweek = VALUES(byeweek)
                """,
                (player_id, player_name, team_name, team_id, position, is_free_agent, injury_status, headshot_url, byeweek)
            )
            logging.info(f"Upserted player data for player_id: {player_id}")

        db.commit()
        logging.info("Player data upsert completed successfully.")

        # Update API usage after successful completion
        update_api_usage(api_calls)

    except Exception as e:
        logging.error(f"An error occurred during player update: {e}")
        logging.error(traceback.format_exc())
    finally:
        cursor.close()
        db.close()
        logging.info("Database connection closed.")

# Run the main player update process
async def main():
    logging.info("Starting player info update.")
    await upsert_player_info()
    logging.info("Player info update completed.")

if __name__ == "__main__":
    asyncio.run(main())