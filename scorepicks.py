import requests
import os
from dotenv import load_dotenv
import datetime
from database import get_db_connection
import logging

logging.basicConfig(
    filename="logs/score_picks.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load environment variables from .env file
load_dotenv()

# Function to update API usage count
def update_api_usage(api_calls):
    current_month_year = datetime.datetime.now().strftime('%Y-%m')

    db = get_db_connection()
    cursor = db.cursor()

    # Check the current usage for the month
    cursor.execute("SELECT request_count FROM api_usage WHERE month_year = %s", (current_month_year,))
    result = cursor.fetchone()

    if result:
        # Update the count by adding the new API calls
        new_count = result[0] + api_calls
        cursor.execute("UPDATE api_usage SET request_count = %s WHERE month_year = %s", (new_count, current_month_year))
    else:
        # Insert a new row for the current month
        cursor.execute("INSERT INTO api_usage (month_year, request_count) VALUES (%s, %s)", (current_month_year, api_calls))

    db.commit()
    cursor.close()
    db.close()

# Function to check if a player has scored and update the game status
def check_player_scores_and_update_game_status():
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    # Initialize API call counter
    api_calls = 0

    # Fetch all picks where 'is_successful' is still 0
    cursor.execute('SELECT * FROM picks WHERE is_successful = 0')
    picks = cursor.fetchall()

    for pick in picks:
        game_id = pick['game_id']
        player_id = pick['player_id']
        pick_id = pick['id']

        # Fetch the game box score data from the API
        api_url = "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/getNFLBoxScore"
        querystring = {"gameID": game_id, "playByPlay": "false"}
        headers = {
            "x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
            "x-rapidapi-host": "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
        }

        response = requests.get(api_url, headers=headers, params=querystring)

        if response.status_code == 200:
            api_calls += 1  # Increment API call count
            game_data = response.json()
            scoring_plays = game_data["body"]["scoringPlays"]
            game_status = game_data["body"]["gameStatus"]
            game_status_code = game_data["body"]["gameStatusCode"]

            # Update the game status and status code in the database
            cursor.execute('''
                UPDATE games 
                SET game_status = %s, game_status_code = %s, last_updated = CURRENT_TIMESTAMP 
                WHERE game_id = %s
            ''', (game_status, game_status_code, game_id))

            # Check if the player has scored a touchdown (TD)
            for play in scoring_plays:
                if play["scoreType"] == "TD" and str(player_id) in play["playerIDs"]:
                    # If player scored, update the 'is_successful' column to 1 only once
                    cursor.execute(
                        'UPDATE picks SET is_successful = 1 WHERE id = %s',
                        (pick_id,)
                    )
                    print(f"Player {player_id} scored in game {game_id}!")

                    # Update leaderboard points only once per player, per pick
                    cursor.execute('''
                        UPDATE leaderboard
                        SET points_week = points_week + 1, total_points = total_points + 1, last_updated = CURRENT_TIMESTAMP
                        WHERE user_id = %s AND week = %s AND points_week = 0
                    ''', (pick['user_id'], pick['week']))

    db.commit()
    cursor.close()
    db.close()

    # Update API usage table with the number of API calls made
    update_api_usage(api_calls)

# Call the function to check scores and update game status
check_player_scores_and_update_game_status()