import sqlite3
from collections import defaultdict

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
    table_names = [table[0] for table in tables]

    # Prefer table named 'requests' if exists
    if 'requests' in table_names:
        table_name = 'requests'
    else:
        # Otherwise choose table with most rows
        max_rows = 0
        for name in table_names:
            cursor.execute(f"SELECT COUNT(*) FROM {name}")
            row_count = cursor.fetchone()[0]
            if row_count > max_rows:
                max_rows = row_count
                table_name = name

    # Get table info
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns_info = cursor.fetchall()
    column_names = [info[1] for info in columns_info]

    # Detect columns
    endpoint_col = find_key(column_names, ['endpoint', 'path', 'route', 'uri'])
    status_col = find_key(column_names, ['status', 'status_code', 'code'])
    latency_col = find_key(column_names, ['latency', 'response_time', 'duration', 'ms'])

    # Fallback if detection fails
    if not endpoint_col:
        endpoint_col = 'endpoint'
    if not status_col:
        status_col = 'status_code'
    if not latency_col:
        latency_col = 'latency_ms'

    # Load data
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()

    # Grouping by endpoint
    endpoint_data = defaultdict(lambda: {'latencies': [], 'total_requests': 0, 'errors': 0})

    for row in rows:
        row_dict = dict(zip(column_names, row))
        endpoint = row_dict.get(endpoint_col)
        status = row_dict.get(status_col)
        latency = row_dict.get(latency_col)

        if endpoint and latency is not None:
            endpoint_data[endpoint]['latencies'].append(latency)
            endpoint_data[endpoint]['total_requests'] += 1
            if status >= 400:
                endpoint_data[endpoint]['errors'] += 1

    # Calculate metrics
    results = []
    for endpoint, data in endpoint_data.items():
        if data['latencies']:
            total_requests = data['total_requests']
            error_rate = data['errors'] / total_requests
            sorted_latencies = sorted(data['latencies'])
            index = int(0.99 * (len(sorted_latencies) - 1))
            p99_latency = sorted_latencies[index]

            results.append({
                'endpoint': endpoint,
                'total_requests': total_requests,
                'error_rate': error_rate,
                'p99_latency': p99_latency
            })

    # Sort and filter results
    results.sort(key=lambda x: x['p99_latency'], reverse=True)
    top_10_results = results[:10]

    # Close the connection
    conn.close()

    return top_10_results

# Example usage
if __name__ == "__main__":
    result = tool()
    for entry in result:
        print(entry)