import aiohttp
import asyncio
import os
import logging
from datetime import datetime
from database import get_db_connection
import traceback

logging.basicConfig(
    filename="logs/injury_check.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Function to update API usage count
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

# Function to fetch injured players from the API
async def fetch_injury_status(player_ids, db, cursor):
    url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLPlayerList"
    headers = {
        "x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
        "x-rapidapi-host": os.getenv("RAPIDAPI_HOST")
    }

    params = {"playerIDs": ",".join(map(str, player_ids))}

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, headers=headers, params=params) as response:
                if response.status == 200:
                    logging.info("Successfully fetched player injury status from API")
                    response_data = await response.json()
                    update_api_usage(1, db, cursor)  # Update API usage count by 1 for this API call
                    return response_data.get('body', [])
                else:
                    error_text = await response.text()
                    logging.error(f"Failed to fetch player list: {response.status} - {error_text}")
                    return None
        except aiohttp.ClientError as e:
            logging.error(f"Error fetching player injury status from API: {e}")
            return None

# Function to update the injury status in the database and trigger a webhook notification
async def update_injury_status(players, db, cursor):
    async with aiohttp.ClientSession() as session:
        for player in players:
            player_id = player.get("playerID")
            injury_status = player["injury"].get("designation", "Unknown")

            if injury_status in ["Out", "Injured Reserve"]:
                logging.info(f"Player {player_id} is listed as {injury_status}, removing pick.")
                try:
                    # Remove the player's pick if status is "Out" or "Injured Reserve"
                    cursor.execute("""
                        DELETE FROM picks
                        WHERE player_id = %s AND is_successful = 0
                    """, (player_id,))
                    logging.info(f"Removed pick for player_id {player_id} due to status: {injury_status}")

                    # Fetch the user IDs who picked the injured player
                    cursor.execute("""
                        SELECT user_id FROM picks WHERE player_id = %s AND is_successful = 0
                    """, (player_id,))
                    tagged_users = [row['user_id'] for row in cursor.fetchall()]

                    # Trigger the webhook to notify users to pick a new player
                    if tagged_users:
                        await send_injury_notification(session, player.get("longName"), injury_status, tagged_users)

                except Exception as e:
                    logging.error(f"Failed to remove pick for player_id {player_id}: {e}")
                    logging.error(traceback.format_exc())

# Function to send injury notification via webhook for players "Out" or "Injured Reserve"
async def send_injury_notification(session, player_name, injury_status, tagged_users):
    url = "http://localhost:3000/webhook/player-injury"
    headers = {
        "Authorization": os.getenv("WEBHOOK_SECRET"),
        "Content-Type": "application/json"
    }
    payload = {
        "playerName": player_name,
        "injuryStatus": injury_status,
        "taggedUsers": tagged_users
    }

    try:
        async with session.post(url, json=payload, headers=headers) as response:
            if response.status == 200:
                logging.info(f"Successfully sent injury notification for player: {player_name}")
            else:
                error_text = await response.text()
                logging.error(f"Failed to send injury notification: {response.status} - {error_text}")
    except aiohttp.ClientError as e:
        logging.error(f"Error sending injury notification: {e}")

# Main execution for injury check
async def main():
    logging.info("Starting player injury status update.")
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)
    try:
        # Fetch the list of player_ids from the picks table where picks are active
        cursor.execute("SELECT DISTINCT player_id FROM picks WHERE is_successful = 0 AND Is_injured = 0")
        player_ids = [row['player_id'] for row in cursor.fetchall()]

        if not player_ids:
            logging.info("No players to check for injury updates.")
            return

        # Fetch the injury status for only the relevant players
        players = await fetch_injury_status(player_ids, db, cursor)
        if players:
            await update_injury_status(players, db, cursor)

        db.commit()
        logging.info("Player injury status update completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during the injury update: {e}")
        logging.error(traceback.format_exc())
    finally:
        cursor.close()
        db.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    asyncio.run(main())