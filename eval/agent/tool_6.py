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

    # Prefer 'requests' table if it exists, otherwise choose the table with the most rows
    table_name = 'requests'
    if table_name not in [t[0] for t in tables]:
        # Find the table with the most rows
        max_rows = 0
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
            row_count = cursor.fetchone()[0]
            if row_count > max_rows:
                max_rows = row_count
                table_name = table[0]

    # Step 3: Load data
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()

    # Get column names
    column_names = [description[0] for description in cursor.description]

    # Step 4: Detect columns
    endpoint_key = find_key(column_names, ["endpoint", "path", "route", "uri"]) or "endpoint"
    status_key = find_key(column_names, ["status", "status_code", "code"]) or "status_code"
    latency_key = find_key(column_names, ["latency", "response_time", "duration", "ms"]) or "latency_ms"

    # Step 5: Grouping
    endpoint_data = {}
    
    for row in rows:
        endpoint = row[column_names.index(endpoint_key)]
        status = row[column_names.index(status_key)]
        latency = row[column_names.index(latency_key)]

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
    top_10_results = results[:10]

    # Step 7: Output format
    if not top_10_results:
        raise ValueError("No valid results found, but data exists.")

    return top_10_results

# Example usage
if __name__ == "__main__":
    output = tool()
    for entry in output:
        print(entry)