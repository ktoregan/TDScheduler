import mysql.connector
import os
import logging
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Discord API helper function
def send_discord_message(DISCORD_CHANNEL_ID, message):
    url = f'https://discord.com/api/channels/{DISCORD_CHANNEL_ID}/messages'
    headers = {
        'Authorization': f'Bot {os.getenv("DISCORD_BOT_TOKEN")}',
        'Content-Type': 'application/json'
    }
    data = {
        'content': message
    }
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        logging.info("Message sent successfully!")
    else:
        logging.error(f"Failed to send message: {response.status_code}")

# Function to build a table format similar to pick history
def format_leaderboard_table(rows, header):
    table = header
    table += '------------------------------------\n'
    
    for row in rows:
        week = str(row['week']).ljust(2)
        player_name = row['player_name'].ljust(26)
        td_status = '✅' if row['is_successful'] else '❌'
        points = str(row['points']).rjust(1)
        table += f'{week} {player_name} {td_status} {points}\n'
    
    return table

# Function to fetch the leaderboard data
def fetch_leaderboard_data():
    try:
        db = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DB"),
            port=os.getenv("MYSQL_PORT"),
            ssl_ca=os.getenv('SSL_CERT_PATH')
        )
        cursor = db.cursor(dictionary=True)

        # Fetch the weekly leaderboard
        cursor.execute("""
            SELECT p.week, pl.player_name, p.is_successful, COUNT(p.is_successful) as points
            FROM picks p
            JOIN players pl ON p.player_id = pl.player_id
            WHERE p.is_successful = 1
            GROUP BY p.user_id, p.week
            ORDER BY p.week;
        """)
        weekly_leaderboard = cursor.fetchall()

        # Fetch the overall leaderboard
        cursor.execute("""
            SELECT u.username, l.total_points
            FROM users u
            JOIN leaderboard l ON u.user_id = l.user_id
            ORDER BY l.total_points DESC;
        """)
        overall_leaderboard = cursor.fetchall()

        cursor.close()
        db.close()

        return weekly_leaderboard, overall_leaderboard

    except mysql.connector.Error as err:
        logging.error(f"Database connection failed: {err}")
        return None, None

# Main function to generate and send the leaderboard
def generate_and_send_leaderboard():
    weekly_leaderboard, overall_leaderboard = fetch_leaderboard_data()
    
    if weekly_leaderboard is None or overall_leaderboard is None:
        logging.error("No leaderboard data to display.")
        return

    # Format weekly leaderboard
    weekly_header = 'Wk Player                     TD P\n'
    formatted_weekly = format_leaderboard_table(weekly_leaderboard, weekly_header)

    # Format overall leaderboard
    overall_table = 'Overall Leaderboard\n'
    overall_table += '-------------------------------\n'
    for idx, row in enumerate(overall_leaderboard, start=1):
        username = row['username'].ljust(15)
        total_points = str(row['total_points']).rjust(5)
        overall_table += f'{idx}. {username} {total_points} pts\n'

    # Combine the two tables
    full_message = f"```\n{formatted_weekly}\n{overall_table}\n```"

    # Send the message to the Discord channel
    send_discord_message(os.getenv("DISCORD_DISCORD_CHANNEL_ID"), full_message)

if __name__ == "__main__":
    logging.basicConfig(
        filename="logs/leaderboard.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    logging.info("Generating and sending leaderboard...")
    generate_and_send_leaderboard()
    logging.info("Leaderboard sent successfully.")