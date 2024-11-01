import requests
import mysql.connector
import os
from dotenv import load_dotenv
import logging
import datetime
from database import get_db_connection

cert_path = os.getenv('SSL_CERT_PATH')

# Load environment variables
load_dotenv()

# Logging setup
logging.basicConfig(
    filename="logs/get_player_info.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Function to fetch team bye week data
def fetch_team_data():
    url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLTeams"
    headers = {
        "x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
        "x-rapidapi-host": os.getenv("RAPIDAPI_HOST")
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        logging.info("Successfully fetched team data from API.")
        update_api_usage(1)  # Increment the usage count by 1 for this API call
        return response.json().get("body", [])
    else:
        logging.error(f"Failed to fetch team data: {response.status_code} {response.text}")
        return None

# Function to update the bye week in the database
def update_player_byeweek(player_id, byeweek):
    try:
        db = get_db_connection()  # Get connection from database module
        cursor = db.cursor()

        query = "UPDATE players SET byeweek = %s, last_updated = CURRENT_TIMESTAMP WHERE player_id = %s"
        cursor.execute(query, (byeweek, player_id))

        db.commit()
        cursor.close()
        db.close()

        logging.info(f"Updated bye week for player_id {player_id} to week {byeweek}")
    except mysql.connector.Error as err:
        logging.error(f"Database operation failed for player_id {player_id}: {err}")

# Fetch player list from the API
def fetch_player_list():
    url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLPlayerList"
    headers = {
        "x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
        "x-rapidapi-host": os.getenv("RAPIDAPI_HOST")
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        logging.info("Successfully fetched player list from API")
        update_api_usage(1)
        return response.json().get("body", [])
    else:
        logging.error(f"Failed to fetch player list: {response.status_code} {response.text}")
        return None

def update_api_usage(api_calls):
    current_month_year = datetime.datetime.now().strftime('%Y-%m')

    db = get_db_connection()
    cursor = db.cursor()

    # Check the current usage for the month
    cursor.execute("SELECT request_count FROM api_usage WHERE month_year = %s", (current_month_year,))
    result = cursor.fetchone()

    if result:
        # Increment the existing request count by the number of new API calls
        new_count = result[0] + api_calls
        cursor.execute("UPDATE api_usage SET request_count = %s, request_time = NOW() WHERE month_year = %s",
                       (new_count, current_month_year))
    else:
        # Insert a new row if no record exists for the current month
        cursor.execute("INSERT INTO api_usage (request_count, request_time, month_year) VALUES (%s, NOW(), %s)",
                       (api_calls, current_month_year))

    db.commit()
    cursor.close()
    db.close()
    logging.info(f"API usage updated. Total requests this month: {new_count if result else api_calls}.")

# Function to notify users about injured players and remove their pick
def notify_injured_players(cursor, player_id, injury_status):
    try:
        # Get users who picked this player
        cursor.execute("SELECT user_id, week FROM picks WHERE player_id = %s AND is_successful = 0 AND Is_injured = 0", (player_id,))
        picks = cursor.fetchall()

        if picks:
            for pick in picks:
                user_id = pick['user_id']
                week = pick['week']
                
                # Update the pick as injured (Is_injured = 1)
                cursor.execute("UPDATE picks SET Is_injured = 1 WHERE player_id = %s AND user_id = %s AND week = %s", (player_id, user_id, week))
                logging.info(f"Player {player_id} is injured. Marked pick for user {user_id} for week {week} as injured. Notifying user.")

                # Send notification to the user (you can integrate your bot here)
                # This part would trigger a message via Discord API or another notification system
                # Example: `bot.send_message(user_id, f"Your pick {player_id} has been injured ({injury_status}). Please select a new player.")`
    except mysql.connector.Error as err:
        logging.error(f"Failed to notify users for injured player {player_id}: {err}")

def upsert_player_info():
    # Fetch the player list from the API
    players = fetch_player_list()
    if players is None:
        logging.error("No player data to process.")
        return

    # Fetch team data from the API (to update bye weeks if necessary)
    teams = fetch_team_data()

    if teams is None:
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
            
            # Convert is_free_agent to integer (1 for True, 0 for False)
            is_free_agent = 1 if player.get("isFreeAgent", "False") == "True" else 0
            injury_status = player["injury"].get("designation", "Healthy")
            headshot_url = player.get("espnHeadshot")

            # Check if player exists in the database
            cursor.execute("SELECT * FROM players WHERE player_id = %s", (player_id,))
            existing_player = cursor.fetchone()

            if existing_player:
                # If player exists, check if the team name has changed
                if existing_player['team_name'] != team_name:
                    logging.info(f"Player {player_name} has changed teams from {existing_player['team_name']} to {team_name}. Updating bye week...")

                    # Find the corresponding team in the team data to update bye week
                    matching_team = next((team for team in teams if team["teamAbv"] == team_name), None)
                    if matching_team:
                        byeweek = matching_team.get("byeWeeks", {}).get("2024", [None])[0]
                        if byeweek:
                            update_player_byeweek(player_id, byeweek)

                # Check if the player is injured with specific statuses and notify users
                if injury_status in ["Doubtful", "Out", "Injured Reserve"]:
                    logging.info(f"Player {player_name} is injured with status {injury_status}. Notifying users...")
                    notify_injured_players(cursor, player_id, injury_status)

                # Perform the upsert operation
                cursor.execute("""
                    UPDATE players
                    SET player_name = %s, team_name = %s, team_id = %s, position = %s, is_free_agent = %s, injury_status = %s, headshot_url = %s, last_updated = CURRENT_TIMESTAMP
                    WHERE player_id = %s
                """, (player_name, team_name, team_id, position, is_free_agent, injury_status, headshot_url, player_id))

            else:
                # If player does not exist, insert new record
                cursor.execute("""
                    INSERT INTO players (player_id, player_name, team_name, team_id, position, is_free_agent, injury_status, headshot_url, last_updated)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """, (player_id, player_name, team_name, team_id, position, is_free_agent, injury_status, headshot_url))

        db.commit()
        cursor.close()
        db.close()
        logging.info("Player information and injury update completed.")

    except mysql.connector.Error as err:
        logging.error(f"Database connection failed: {err}")

# Main execution
if __name__ == "__main__":
    logging.info("Starting TD Showdown player info, injury check, and bye week update.")
    upsert_player_info()
    logging.info("Player info, injury check, and bye week update completed successfully.")