import glob
import re
import json
from collections import defaultdict

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

def tool():
    # Step 1: File discovery
    files = glob.glob('data/**/*.log', recursive=True) + glob.glob('**/*.log', recursive=True)
    files = list(set(files))  # Remove duplicates

    if not files:
        # Fallback to known filenames
        known_files = ['employees.json', 'sales.csv', 'app.log']
        for known_file in known_files:
            if glob.glob(known_file):
                files.append(known_file)

        # Fallback to os.walk
        import os
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.log'):
                    files.append(os.path.join(root, filename))

        files = list(set(files))  # Remove duplicates

    if not files:
        return "No matching file found"

    # Step 2: Initialize data structures
    log_data = defaultdict(list)

    # Step 3: Parse log files
    for file in files:
        with open(file, 'r') as f:
            for line in f:
                parts = line.strip().split()
                data = {}
                for p in parts:
                    if '=' in p:
                        k, v = p.split('=', 1)
                        data[k] = v

                # Step 4: Extract fields
                method = data.get("method", "GET")
                endpoint = data.get("endpoint")
                status = int(data.get("status", 200))

                # Extract latency
                raw = data.get("latency_ms", "0")
                m = re.search(r'(\d+\.?\d*)', raw)
                latency = float(m.group(1)) if m else 0

                if endpoint:
                    log_data[(method, endpoint)].append((status, latency))

    # Step 5: Grouping and calculations
    results = []
    for (method, endpoint), entries in log_data.items():
        total_requests = len(entries)
        latencies = [latency for _, latency in entries]
        error_count = sum(1 for status, _ in entries if status >= 400)
        error_rate = (error_count / total_requests) * 100 if total_requests > 0 else 0
        avg_latency = sum(latencies) / total_requests if total_requests > 0 else 0

        # Calculate P95 latency
        values = sorted(latencies)
        index = int(0.95 * (len(values) - 1)) if values else 0
        p95_latency = values[index] if values else 0

        results.append({
            "method": method,
            "endpoint": endpoint,
            "total_requests": total_requests,
            "avg_latency": avg_latency,
            "error_rate": error_rate,
            "p95_latency": p95_latency
        })

    # Step 6: Sort by error rate descending
    results.sort(key=lambda x: x['error_rate'], reverse=True)

    # Step 7: Return top 5 groups
    return results[:5]