import mysql.connector
from crontab import CronTab
from datetime import datetime, timedelta, date
import os
from database import get_db_connection
import logging

# Configure logging
logging.basicConfig(filename="logs/create-game-schedule.log", level=logging.INFO, format="%(asctime)s - %(message)s")

# Connect to the database and fetch game times
def get_game_times_for_week(week):
    try:
        db = get_db_connection()
        cursor = db.cursor(dictionary=True)
        query = """
            SELECT game_time, game_id
            FROM games
            WHERE week = %s AND game_time IS NOT NULL
        """
        cursor.execute(query, (week,))
        game_times = [(row['game_time'], row['game_id']) for row in cursor.fetchall()]
        cursor.close()
        db.close()
        return game_times
    except mysql.connector.Error as err:
        logging.error(f"Database connection failed: {err}")
        return []

# Function to add a cron job dynamically
def add_cron_job(script_name, run_time, week, game_id):
    cron_user = os.getenv("USER") or os.getlogin()
    cron = CronTab(user=cron_user)
    job = cron.new(command=f'python {script_name}', comment=f'week_{week}_game_{game_id}')
    job.setall(run_time)
    cron.write()
    logging.info(f"Added cron job for {script_name} at {run_time} for week {week}, game {game_id}")

# Function to remove all cron jobs related to the previous week
def remove_old_jobs(week):
    cron_user = os.getenv("USER") or os.getlogin()
    cron = CronTab(user=cron_user)
    for job in cron:
        if f'week_{week - 1}' in job.comment:
            cron.remove(job)
            logging.info(f"Removed cron job: {job}")
    cron.write()

# Main function to schedule tasks for each game
def schedule_tasks_for_week(week):
    # Remove old jobs for the previous week
    if week > 7:
        remove_old_jobs(week - 1)

    game_times = get_game_times_for_week(week)
    if not game_times:
        logging.info(f"No games found for week {week}.")
        return

    for game_time_str, game_id in game_times:
        game_time = datetime.strptime(game_time_str, "%Y-%m-%d %H:%M:%S")

        # Schedule scheduleUpdate.py and playerUpdate.py 2 hours before game start
        run_time_update = game_time - timedelta(hours=2)
        add_cron_job("scheduleUpdate.py", run_time_update, week, game_id)
        add_cron_job("playerUpdate.py", run_time_update, week, game_id)
        logging.info(f"Scheduled scheduleUpdate.py and playerUpdate.py for {run_time_update}.")

        # Schedule injuryCheck.py at 1hr, 45mins, 30mins, and 15mins before game start
        for minutes_before in [60, 45, 30, 15]:
            run_time_injury = game_time - timedelta(minutes=minutes_before)
            add_cron_job("injuryCheck.py", run_time_injury, week, game_id)
            logging.info(f"Scheduled injuryCheck.py for {run_time_injury}.")

if __name__ == "__main__":
    today = datetime.now().date()
    # Determine the current week
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

    current_week = None
    for week, start_date in week_mapping.items():
        if today >= start_date:
            current_week = week
        else:
            break

    if current_week:
        schedule_tasks_for_week(current_week)