def tool():
    import sqlite3
    import json

    # Step 1: Connect to the database
    path = 'data/database.db'  # Adjust the path as necessary
    conn = sqlite3.connect(path)
    cursor = conn.cursor()

    # Step 2: Discover schema
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()

    target_table = None
    max_rows = 0

    for (table_name,) in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        if table_name == 'requests':
            target_table = table_name
            break
        elif row_count > max_rows:
            max_rows = row_count
            target_table = table_name

    if target_table is None:
        return []

    # Step 3: Load data
    cursor.execute(f"SELECT * FROM {target_table}")
    rows = cursor.fetchall()

    # Step 4: Detect columns
    column_names = [description[0] for description in cursor.description]
    endpoint_col = find_key(column_names, ['endpoint', 'path', 'route', 'uri'])
    status_col = find_key(column_names, ['status', 'status_code', 'code'])
    latency_col = find_key(column_names, ['latency', 'response_time', 'duration', 'ms'])

    # Fallback if detection fails
    if endpoint_col is None:
        endpoint_col = 'endpoint'
    if status_col is None:
        status_col = 'status_code'
    if latency_col is None:
        latency_col = 'latency_ms'

    # Step 5: Grouping
    endpoint_data = {}
    
    for row in rows:
        endpoint = row[column_names.index(endpoint_col)]
        status = row[column_names.index(status_col)]
        latency = row[column_names.index(latency_col)]

        if endpoint not in endpoint_data:
            endpoint_data[endpoint] = {
                'total_requests': 0,
                'error_count': 0,
                'latencies': []
            }

        endpoint_data[endpoint]['total_requests'] += 1
        if status >= 400:
            endpoint_data[endpoint]['error_count'] += 1
        if isinstance(latency, (int, float)) and latency is not None:
            endpoint_data[endpoint]['latencies'].append(latency)

    # Step 6: Sort + Filter
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

    results.sort(key=lambda x: x['p99_latency'], reverse=True)
    top_results = results[:10]

    # Step 7: Output format
    return top_results

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None