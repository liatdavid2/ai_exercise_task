import sqlite3

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

def tool():
    # Connect to the SQLite database
    path = 'data/metrics.db'
    conn = sqlite3.connect(path)
    cursor = conn.cursor()

    # Discover schema
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    table_name = None

    # Prefer table named 'requests' if exists
    for table in tables:
        if table[0] == 'requests':
            table_name = 'requests'
            break

    # If 'requests' table doesn't exist, choose the table with the most rows
    if not table_name:
        max_rows = 0
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
            row_count = cursor.fetchone()[0]
            if row_count > max_rows:
                max_rows = row_count
                table_name = table[0]

    # Load data from the chosen table
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()

    # Get column names
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    column_names = [col[1] for col in columns]

    # Detect columns
    endpoint_key = find_key(column_names, ["endpoint", "path", "route", "uri"]) or "endpoint"
    status_key = find_key(column_names, ["status", "status_code", "code"]) or "status_code"
    latency_key = find_key(column_names, ["latency", "response_time", "duration", "ms"]) or "latency_ms"

    # Grouping and calculations
    endpoint_data = {}
    for row in rows:
        row_dict = dict(zip(column_names, row))
        endpoint = row_dict.get(endpoint_key)
        status = row_dict.get(status_key)
        latency = row_dict.get(latency_key)

        if endpoint and latency is not None:
            if endpoint not in endpoint_data:
                endpoint_data[endpoint] = {'latencies': [], 'total_requests': 0, 'error_count': 0}

            endpoint_data[endpoint]['latencies'].append(latency)
            endpoint_data[endpoint]['total_requests'] += 1
            if status >= 400:
                endpoint_data[endpoint]['error_count'] += 1

    # Prepare the result
    result = []
    for endpoint, data in endpoint_data.items():
        if data['latencies']:
            total_requests = data['total_requests']
            error_rate = data['error_count'] / total_requests
            sorted_latencies = sorted(data['latencies'])
            index = int(0.99 * (len(sorted_latencies) - 1))
            p99_latency = sorted_latencies[index]

            result.append({
                'endpoint': endpoint,
                'total_requests': total_requests,
                'error_rate': error_rate,
                'p99_latency': p99_latency
            })

    # Sort by p99_latency DESC and return top 10
    result.sort(key=lambda x: x['p99_latency'], reverse=True)
    return result[:10]

# Example usage
if __name__ == "__main__":
    top_endpoints = tool()
    for endpoint in top_endpoints:
        print(endpoint)