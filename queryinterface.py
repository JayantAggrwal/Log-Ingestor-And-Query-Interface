from flask import Flask, request, jsonify
import sqlite3

app = Flask(__name__)

DATABASE = 'logs.db'

def query_logs(filters):
    with sqlite3.connect(DATABASE) as connection:
        cursor = connection.cursor()
        
        # Build the query dynamically based on provided filters
        query = "SELECT * FROM logs WHERE 1=1"
        values = []
        for key, value in filters.items():
            query += f" AND {key} = ?"
            values.append(value)

        cursor.execute(query, tuple(values))
        result = cursor.fetchall()

    return result

@app.route('/search', methods=['GET'])
def search_logs():
    try:
        filters = request.args.to_dict()

        # Validate and sanitize input parameters if needed

        # Apply filters
        filtered_logs = query_logs(filters)

        return jsonify({"logs": filtered_logs})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(port=3000)
