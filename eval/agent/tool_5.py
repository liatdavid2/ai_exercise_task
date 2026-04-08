import glob
import os
import re
from collections import defaultdict

def find_key(d, options):
    for k in d:
        for opt in options:
            if opt in k.lower():
                return k
    return None

def tool():
    # Step 1: Discover files
    files = glob.glob('data/**/*.log', recursive=True) + glob.glob('**/*.log', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    files = sorted(files, key=lambda x: (not x.startswith('data/'), x))

    if not files:
        # Fallback to os.walk if no files found
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.log'):
                    files.append(os.path.join(root, filename))
        if not files:
            return "No matching file found"

    # Step 2: Initialize data structures
    endpoint_data = defaultdict(lambda: {
        "total_requests": 0,
        "latencies": [],
        "error_count": 0
    })

    # Step 3: Process each file
    for file in files:
        with open(file, 'r') as f:
            for line in f:
                parts = line.strip().split()
                data = {}
                for p in parts:
                    if '=' in p:
                        k, v = p.split('=', 1)
                        data[k] = v

                # Step 4: Extract core fields
                method_key = find_key(data, ['method', 'http_method', 'verb'])
                endpoint_key = find_key(data, ['endpoint', 'path', 'route', 'url', 'uri'])
                status_key = find_key(data, ['status', 'status_code', 'code', 'response_code'])
                latency_key = find_key(data, ['latency_ms', 'latency', 'response_time_ms', 'response_time', 'duration_ms', 'duration'])

                method = data.get(method_key, '').strip()
                endpoint = data.get(endpoint_key, '').strip()
                status = data.get(status_key, '').strip()
                raw_latency = data.get(latency_key, '').strip()

                # Step 5: Safe parsing
                if not method or not endpoint:
                    continue

                try:
                    status = int(status)
                except:
                    continue

                latency = None
                if raw_latency:
                    m = re.search(r'(\d+(?:\.\d+)?)', raw_latency)
                    if m:
                        latency = float(m.group(1))

                # Step 6: Grouping
                key = (method, endpoint)
                endpoint_data[key]["total_requests"] += 1
                if latency is not None:
                    endpoint_data[key]["latencies"].append(latency)
                if status >= 400:
                    endpoint_data[key]["error_count"] += 1

    # Step 7: Compute metrics
    results = []
    for (method, endpoint), data in endpoint_data.items():
        total_requests = data["total_requests"]
        latencies = data["latencies"]
        error_count = data["error_count"]

        avg_latency = sum(latencies) / len(latencies) if latencies else 0
        error_rate = (error_count / total_requests) * 100 if total_requests else 0

        # Compute p95 latency
        if latencies:
            latencies.sort()
            index = int(0.95 * (len(latencies) - 1))
            p95_latency = latencies[index]
        else:
            p95_latency = 0

        results.append({
            "method": method,
            "endpoint": endpoint,
            "total_requests": total_requests,
            "avg_latency": round(avg_latency, 2),
            "error_rate": round(error_rate, 2),
            "p95_latency": round(p95_latency, 2)
        })

    # Step 8: Sorting and output
    results.sort(key=lambda x: (-x["error_rate"], -x["total_requests"]))
    return results[:5]