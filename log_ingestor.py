from flask import Flask, request, jsonify
import sqlite3
import asyncio
from gevent.pywsgi import WSGIServer

app = Flask(__name__)

DATABASE = 'logs.db'

# Create the logs table if it doesn't exist
with sqlite3.connect(DATABASE) as connection:
    cursor = connection.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            level TEXT,
            message TEXT,
            resourceId TEXT,
            timestamp TEXT,
            traceId TEXT,
            spanId TEXT,
            commit TEXT,
            parentResourceId TEXT
        )
    ''')

async def ingest_log(log_data):
    await asyncio.sleep(0)  # Simulate an asynchronous operation
    with sqlite3.connect(DATABASE) as connection:
        cursor = connection.cursor()
        cursor.execute('''
    INSERT INTO logs (level, message, resourceId, timestamp, traceId, spanId, "commit", parentResourceId)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
    log_data.get('level'),
    log_data.get('message'),
    log_data.get('resourceId'),
    log_data.get('timestamp'),
    log_data.get('traceId'),
    log_data.get('spanId'),
    log_data.get('commit'),
    log_data.get('metadata', {}).get('parentResourceId')
    ))

    connection.commit()

@app.route('/ingest', methods=['POST'])
def handle_ingest():
    log_data = request.get_json()
    asyncio.ensure_future(ingest_log(log_data))
    return jsonify({"message": "Log ingestion initiated"})

if __name__ == '__main__':
    http_server = WSGIServer(('0.0.0.0', 3000), app)
    print("Log Ingestor is running on http://localhost:3000/ingest")
    http_server.serve_forever()
