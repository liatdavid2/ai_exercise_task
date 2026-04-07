def tool():
    import sqlite3

    # Step 1: Connect to the SQLite database
    conn = sqlite3.connect('data/metrics.db')
    cursor = conn.cursor()

    # Step 2: Discover tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()

    # Step 3: Inspect schema for the 'requests' table
    requests_table = 'requests'
    if (requests_table,) in tables:
        cursor.execute(f"PRAGMA table_info({requests_table})")
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]

        # Step 4: Load all rows into Python
        cursor.execute(f"SELECT * FROM {requests_table}")
        rows = cursor.fetchall()

        # Step 5: Detect useful columns dynamically
        p99_key = find_key(column_names, ['p99'])
        status_code_key = find_key(column_names, ['status_code'])
        endpoint_key = find_key(column_names, ['endpoint'])
        
        if p99_key and status_code_key and endpoint_key:
            # Prepare to calculate total request count and error rate
            endpoint_stats = {}
            for row in rows:
                endpoint = row[column_names.index(endpoint_key)]
                p99_latency = row[column_names.index(p99_key)]
                status_code = row[column_names.index(status_code_key)]

                if endpoint not in endpoint_stats:
                    endpoint_stats[endpoint] = {
                        'total_requests': 0,
                        'error_count': 0,
                        'p99_latency': 0
                    }

                endpoint_stats[endpoint]['total_requests'] += 1
                endpoint_stats[endpoint]['p99_latency'] += p99_latency
                if status_code >= 400:
                    endpoint_stats[endpoint]['error_count'] += 1

            # Calculate average p99 latency and error rate
            results = []
            for endpoint, stats in endpoint_stats.items():
                avg_p99_latency = stats['p99_latency'] / stats['total_requests']
                error_rate = stats['error_count'] / stats['total_requests'] if stats['total_requests'] > 0 else 0
                results.append((endpoint, avg_p99_latency, stats['total_requests'], error_rate))

            # Step 6: Return structured non-empty output when data exists
            results.sort(key=lambda x: x[1], reverse=True)  # Sort by p99 latency descending
            return results[:10]  # Return top 10 endpoints

    conn.close()
    return []  # Return empty if no relevant data found

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None