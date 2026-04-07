import sqlite3

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

def tool():
    # Step 1: Connect to the SQLite database
    conn = sqlite3.connect('data/metrics.db')
    cursor = conn.cursor()

    # Step 2: Discover schema
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()

    # Prefer 'requests' table if it exists
    table_name = 'requests' if ('requests',) in tables else tables[0][0]

    # Get the columns of the chosen table
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns_info = cursor.fetchall()
    columns = [col[1] for col in columns_info]

    # Step 3: Load data
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()

    # Step 4: Detect columns
    endpoint_key = find_key(columns, ["endpoint", "path", "route", "uri"]) or "endpoint"
    status_key = find_key(columns, ["status", "status_code", "code"]) or "status_code"
    latency_key = find_key(columns, ["latency", "response_time", "duration", "ms"]) or "latency_ms"

    # Step 5: Grouping
    endpoint_data = {}
    
    for row in rows:
        endpoint = row[columns.index(endpoint_key)]
        status = row[columns.index(status_key)]
        latency = row[columns.index(latency_key)]

        if endpoint not in endpoint_data:
            endpoint_data[endpoint] = {
                'total_requests': 0,
                'error_count': 0,
                'latencies': []
            }

        endpoint_data[endpoint]['total_requests'] += 1
        if status >= 400:
            endpoint_data[endpoint]['error_count'] += 1
        if isinstance(latency, (int, float)):
            endpoint_data[endpoint]['latencies'].append(latency)

    # Step 6: Calculate metrics and filter
    results = []
    for endpoint, data in endpoint_data.items():
        if data['latencies']:
            total_requests = data['total_requests']
            error_rate = data['error_count'] / total_requests
            p99_latency = sorted(data['latencies'])[int(0.99 * (len(data['latencies']) - 1))]
            results.append({
                'endpoint': endpoint,
                'total_requests': total_requests,
                'error_rate': error_rate,
                'p99_latency': p99_latency
            })

    # Sort by p99_latency DESC and get top 10
    results.sort(key=lambda x: x['p99_latency'], reverse=True)
    top_results = results[:10]

    # Close the database connection
    conn.close()

    return top_results if top_results else []