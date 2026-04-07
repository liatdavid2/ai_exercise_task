import glob
import json
import os
import re
from collections import defaultdict

def tool():
    # Step 1: File discovery
    files = glob.glob('data/**/*.log', recursive=True) + glob.glob('**/*.log', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    if not files:
        # Fallback: known filenames
        known_files = ['employees.json', 'sales.csv', 'app.log']
        for known_file in known_files:
            if os.path.exists(f'data/{known_file}'):
                files.append(f'data/{known_file}')
            elif os.path.exists(known_file):
                files.append(known_file)

        # Fallback: os.walk
        if not files:
            for root, _, filenames in os.walk('data/'):
                for filename in filenames:
                    if filename.endswith('.log'):
                        files.append(os.path.join(root, filename))

    if not files:
        return "No matching file found"

    # Step 2: Log analysis
    log_data = defaultdict(lambda: {'total_requests': 0, 'latencies': [], 'error_count': 0})

    for file in files:
        with open(file, 'r') as f:
            for line in f:
                parts = line.strip().split()
                data = {}
                for p in parts:
                    if '=' in p:
                        k, v = p.split('=', 1)
                        data[k] = v

                method = data.get("method", "GET")
                endpoint = data.get("endpoint")
                status = int(data.get("status", 200))

                # Handle latency
                raw = data.get("latency_ms", "0")
                m = re.search(r'(\d+\.?\d*)', raw)
                latency = float(m.group(1)) if m else 0

                # Fallback for missing endpoint
                if not endpoint:
                    m = re.search(r'/api/\S+', line)
                    endpoint = m.group(0) if m else None

                if endpoint:
                    log_data[(method, endpoint)]['total_requests'] += 1
                    log_data[(method, endpoint)]['latencies'].append(latency)
                    if status >= 400:
                        log_data[(method, endpoint)]['error_count'] += 1

    # Step 3: Calculate metrics
    results = []
    for (method, endpoint), metrics in log_data.items():
        total_requests = metrics['total_requests']
        latencies = metrics['latencies']
        error_count = metrics['error_count']

        if total_requests > 0:
            avg_latency = sum(latencies) / total_requests
            error_rate = (error_count / total_requests) * 100
            p95_latency = sorted(latencies)[int(0.95 * (len(latencies) - 1))] if latencies else 0

            results.append({
                "method": method,
                "endpoint": endpoint,
                "total_requests": total_requests,
                "avg_latency": avg_latency,
                "error_rate": error_rate,
                "p95_latency": p95_latency
            })

    # Step 4: Sort results by error rate DESC and return top 5
    results.sort(key=lambda x: x['error_rate'], reverse=True)
    return results[:5]