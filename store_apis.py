import csv
from turtle import pd
import uuid
import pytz
from flask import Flask, jsonify, request
from threading import Thread
from datetime import datetime, timedelta
from pymongo import MongoClient

app = Flask(__name__)

client = MongoClient("mongodb+srv://raj:raj@cluster0.wsrp5rf.mongodb.net/?retryWrites=true&w=majority")  # Replace with your MongoDB connection details
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)
db = client.store_monitoring_db
# Global variable to store the report status and data
report_status = {}
report_data = {}

# Helper function to calculate the overlapping time between two time intervals
def calculate_overlap(start_time1, end_time1, start_time2, end_time2):
    overlap_start = max(start_time1, start_time2)
    overlap_end = min(end_time1, end_time2)
    overlap = max(0, (overlap_end - overlap_start).seconds // 60)
    return overlap

# Helper function to calculate uptime and downtime for a given store, day, and time interval
def calculate_uptime_downtime(store_id, day, start_time, end_time):
    total_uptime = 0
    total_downtime = 0

    # Retrieve the store's business hours
    business_hours = db.business_hours.find_one({'store_id': store_id, 'dayOfWeek': day})
    if not business_hours:
        # Store is open 24*7, consider the entire interval as uptime
        total_uptime = (end_time - start_time).seconds // 60
    else:
        # Convert local time to UTC based on the store's timezone
        timezone = db.timezones.find_one({'store_id': store_id})
        if timezone:
            timezone_str = timezone['timezone_str']
        else:
            timezone_str = 'America/Chicago'  # Default timezone if not specified

        start_time_utc = start_time.astimezone(pytz.timezone(timezone_str))
        end_time_utc = end_time.astimezone(pytz.timezone(timezone_str))

        # Iterate over the store's status data within the given time interval
        cursor = db.stores.find({'store_id': store_id, 'timestamp_utc': {'$gte': start_time_utc, '$lt': end_time_utc}})
        prev_status = None
        prev_timestamp = None
        for data in cursor:
            status = data['status']
            timestamp = data['timestamp_utc']
            if prev_status is not None:
                # Calculate the overlapping time between consecutive status observations
                overlap = calculate_overlap(prev_timestamp, timestamp, start_time_utc, end_time_utc)
                if prev_status == 'active':
                    total_uptime += overlap
                else:
                    total_downtime += overlap
            prev_status = status
            prev_timestamp = timestamp

        # Handle the last observation
        if prev_status is not None:
            last_interval_overlap = calculate_overlap(prev_timestamp, end_time_utc, start_time_utc, end_time_utc)
            if prev_status == 'active':
                total_uptime += last_interval_overlap
            else:
                total_downtime += last_interval_overlap

    return total_uptime, total_downtime

def ingest_csv_data():
    # Load and store data from CSV 1
    csv1_data = pd.read_csv('storestatus.csv')
    store_data = csv1_data.to_dict(orient='records')
    db.stores.insert_many(store_data)

    # Load and store data from CSV 2
    csv2_data = pd.read_csv('MenuHours.csv')
    business_hours_data = csv2_data.to_dict(orient='records')
    db.business_hours.insert_many(business_hours_data)

    # Load and store data from CSV 3
    csv3_data = pd.read_csv('Timezone.csv')
    timezone_data = csv3_data.to_dict(orient='records')
    db.timezones.insert_many(timezone_data)
# Generate the report based on the stored data

def generate_report():
    global report_status
    global report_data
    ingest_csv_data()
    # Get the current UTC timestamp as the max timestamp among all observations
    max_timestamp = db.stores.find_one(sort=[('timestamp_utc', -1)])['timestamp_utc']

    # Retrieve all store IDs
    store_ids = db.stores.distinct('store_id')
    print(len(store_ids))
    report = []
    i=0
    for store_id in store_ids:
        report_entry = {'store_id': store_id}

        # Calculate uptime and downtime for the last hour, day, and week
        current_time = datetime.utcnow()
        one_hour_ago = current_time - timedelta(hours=1)
        one_day_ago = current_time - timedelta(days=1)
        one_week_ago = current_time - timedelta(weeks=1)

        uptime_last_hour, downtime_last_hour = calculate_uptime_downtime(store_id, current_time.weekday(), one_hour_ago, current_time)
        uptime_last_day, downtime_last_day = calculate_uptime_downtime(store_id, current_time.weekday(), one_day_ago, current_time)
        uptime_last_week, downtime_last_week = calculate_uptime_downtime(store_id, None, one_week_ago, current_time)
        report_entry['uptime_last_hour'] = uptime_last_hour
        report_entry['downtime_last_hour'] = downtime_last_hour
        report_entry['uptime_last_day'] = uptime_last_day
        report_entry['downtime_last_day'] = downtime_last_day
        report_entry['uptime_last_week'] = uptime_last_week
        report_entry['downtime_last_week'] = downtime_last_week
        report.append(report_entry)
        i=i+1

    report_data['report'] = report
    report_data['timestamp'] = max_timestamp
    report_status['status'] = 'Complete'

# Endpoint to trigger report generation
@app.route('/trigger_report', methods=['POST'])
def trigger_report():
    global report_status
    global report_data

    # Reset report status and data
    report_status['status'] = 'Running'
    report_data.clear()

    # Start a new thread to generate the report
    thread = Thread(target=generate_report)
    thread.start()

    # Generate a unique report ID
    report_id = str(uuid.uuid4())

    # Return the report ID
    return jsonify({'report_id': report_id})

# Endpoint to get the report status or the CSV file
@app.route('/get_report', methods=['GET'])
def get_report():
    report_id = request.args.get('report_id')

    if report_status['status'] == 'Running':
        return jsonify({'status': 'Running'})

    if report_status['status'] == 'Complete':
        # Generate the CSV file
        report = report_data['report']
        timestamp = report_data['timestamp']

        csv_data = [['store_id', 'uptime_last_hour', 'downtime_last_hour', 'uptime_last_day', 'downtime_last_day', 'uptime_last_week', 'downtime_last_week']]
        for entry in report:
            csv_data.append([
                entry['store_id'],
                entry['uptime_last_hour'],
                entry['downtime_last_hour'],
                entry['uptime_last_day'],
                entry['downtime_last_day'],
                entry['uptime_last_week'],
                entry['downtime_last_week']
            ])

        # Save CSV data to a temporary file
        temp_file_path = f'report.csv'
        with open(temp_file_path, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(csv_data)

        # Return the CSV file path and status
        return jsonify({'status': 'Complete', 'file_path': temp_file_path})

# Run the Flask app
if __name__ == '__main__':
    app.run()
