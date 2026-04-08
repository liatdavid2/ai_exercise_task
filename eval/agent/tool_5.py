import sqlite3

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

def tool():
    # Step 1: Connect to the SQLite database
    path = 'data/metrics.db'
    conn = sqlite3.connect(path)
    cursor = conn.cursor()

    # Step 2: Discover schema
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    table_names = [table[0] for table in tables]

    # Prefer table named 'requests' if exists
    if 'requests' in table_names:
        target_table = 'requests'
    else:
        # Otherwise choose table with most rows
        max_rows = 0
        target_table = None
        for table in table_names:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            row_count = cursor.fetchone()[0]
            if row_count > max_rows:
                max_rows = row_count
                target_table = table

    # Step 3: Load data
    cursor.execute(f"SELECT * FROM {target_table}")
    rows = cursor.fetchall()

    # Get column names
    cursor.execute(f"PRAGMA table_info({target_table})")
    columns_info = cursor.fetchall()
    column_names = [info[1] for info in columns_info]

    # Step 4: Detect columns
    endpoint_key = find_key(column_names, ["endpoint", "path", "route", "uri"]) or "endpoint"
    status_key = find_key(column_names, ["status", "status_code", "code"]) or "status_code"
    latency_key = find_key(column_names, ["latency", "response_time", "duration", "ms"]) or "latency_ms"

    # Step 5: Grouping
    endpoint_data = {}
    for row in rows:
        row_dict = dict(zip(column_names, row))
        endpoint = row_dict.get(endpoint_key)
        status = row_dict.get(status_key)
        latency = row_dict.get(latency_key)

        if endpoint and isinstance(latency, (int, float)):
            if endpoint not in endpoint_data:
                endpoint_data[endpoint] = {'latencies': [], 'total_requests': 0, 'error_count': 0}
            endpoint_data[endpoint]['latencies'].append(latency)
            endpoint_data[endpoint]['total_requests'] += 1
            if status >= 400:
                endpoint_data[endpoint]['error_count'] += 1

    # Prepare results
    results = []
    for endpoint, data in endpoint_data.items():
        if data['latencies']:
            total_requests = data['total_requests']
            error_rate = data['error_count'] / total_requests
            sorted_latencies = sorted(data['latencies'])
            index = int(0.99 * (len(sorted_latencies) - 1))
            p99_latency = sorted_latencies[index]
            results.append({
                'endpoint': endpoint,
                'total_requests': total_requests,
                'error_rate': error_rate,
                'p99_latency': p99_latency
            })

    # Step 6: Sort + Filter
    results.sort(key=lambda x: x['p99_latency'], reverse=True)
    top_10_results = results[:10]

    # Step 7: Output format
    return top_10_results

# Example usage
if __name__ == "__main__":
    result = tool()
    for entry in result:
        print(entry)