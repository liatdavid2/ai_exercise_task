import sqlite3

def tool():
    # Connect to the SQLite database
    conn = sqlite3.connect('data/metrics.db')
    cursor = conn.cursor()

    # Discover the schema
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()

    # Determine the table to use
    table_name = None
    max_rows = 0
    for (table,) in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        row_count = cursor.fetchone()[0]
        if table == 'requests' or row_count > max_rows:
            table_name = table
            max_rows = row_count

    # If no table found, return empty result
    if not table_name:
        return []

    # Get table schema
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns_info = cursor.fetchall()
    columns = [col[1] for col in columns_info]

    # Detect relevant columns
    endpoint_col = find_key(columns, ["endpoint", "path", "route", "uri"]) or "endpoint"
    status_col = find_key(columns, ["status", "status_code", "code"]) or "status_code"
    latency_col = find_key(columns, ["latency", "response_time", "duration", "ms"]) or "latency_ms"

    # Load data from the table
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()

    # Process data
    endpoint_data = {}
    for row in rows:
        row_dict = dict(zip(columns, row))
        endpoint = row_dict.get(endpoint_col)
        status = row_dict.get(status_col)
        latency = row_dict.get(latency_col)

        if endpoint and latency is not None:
            if endpoint not in endpoint_data:
                endpoint_data[endpoint] = {'latencies': [], 'total_requests': 0, 'error_count': 0}

            endpoint_data[endpoint]['latencies'].append(latency)
            endpoint_data[endpoint]['total_requests'] += 1
            if status >= 400:
                endpoint_data[endpoint]['error_count'] += 1

    # Calculate metrics for each endpoint
    results = []
    for endpoint, data in endpoint_data.items():
        if not data['latencies']:
            continue

        total_requests = data['total_requests']
        error_rate = data['error_count'] / total_requests
        sorted_latencies = sorted(data['latencies'])
        p99_index = int(0.99 * (len(sorted_latencies) - 1))
        p99_latency = sorted_latencies[p99_index]

        results.append({
            'endpoint': endpoint,
            'total_requests': total_requests,
            'error_rate': error_rate,
            'p99_latency': p99_latency
        })

    # Sort results by p99_latency descending and return top 10
    results.sort(key=lambda x: x['p99_latency'], reverse=True)
    return results[:10]

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None