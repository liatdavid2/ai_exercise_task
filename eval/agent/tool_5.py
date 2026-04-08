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
    # Step 1: Discover log files
    files = glob.glob('data/**/*.log', recursive=True) + glob.glob('**/*.log', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    files = sorted(files, key=lambda x: 0 if x.startswith('data/') else 1)

    if not files:
        # Fallback to os.walk if no files found
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.log'):
                    files.append(os.path.join(root, filename))
        if not files:
            return "No matching file found"

    # Step 2: Parse and analyze log data
    method_key_options = ["method", "http_method", "verb"]
    endpoint_key_options = ["endpoint", "path", "route", "url", "uri"]
    status_key_options = ["status", "status_code", "code", "response_code"]
    latency_key_options = ["latency_ms", "latency", "response_time_ms", "response_time", "duration_ms", "duration"]

    endpoint_stats = defaultdict(lambda: {
        "total_requests": 0,
        "latencies": [],
        "error_count": 0
    })

    for file in files:
        with open(file, 'r') as f:
            for line in f:
                parts = line.strip().split()
                data = {}
                for p in parts:
                    if '=' in p:
                        k, v = p.split('=', 1)
                        data[k.strip()] = v.strip()

                # Extract core fields
                method = data.get(find_key(data, method_key_options))
                endpoint = data.get(find_key(data, endpoint_key_options))
                status = data.get(find_key(data, status_key_options))
                raw_latency = data.get(find_key(data, latency_key_options))

                # Fallback for endpoint using regex if missing
                if not endpoint:
                    match = re.search(r'/api/\S*', line)
                    if match:
                        endpoint = match.group(0)

                if not method or not endpoint:
                    continue  # Skip if essential fields are missing

                # Safe parsing of status
                try:
                    status = int(status)
                except (ValueError, TypeError):
                    continue

                # Safe parsing of latency
                latency = None
                if raw_latency:
                    raw_latency = str(raw_latency).strip()
                    m = re.search(r'(\d+(?:\.\d+)?)', raw_latency)
                    if m:
                        latency = float(m.group(1))

                # Update statistics
                key = (method, endpoint)
                endpoint_stats[key]["total_requests"] += 1
                if latency is not None:
                    endpoint_stats[key]["latencies"].append(latency)
                if status >= 400:
                    endpoint_stats[key]["error_count"] += 1

    # Step 3: Compute metrics for each group
    results = []
    for (method, endpoint), stats in endpoint_stats.items():
        total_requests = stats["total_requests"]
        error_rate = stats["error_count"] / total_requests if total_requests > 0 else 0
        avg_latency = sum(stats["latencies"]) / len(stats["latencies"]) if stats["latencies"] else 0
        latencies_sorted = sorted(stats["latencies"])
        p95_latency = latencies_sorted[int(0.95 * (len(latencies_sorted) - 1))] if latencies_sorted else 0

        results.append({
            "method": method,
            "endpoint": endpoint,
            "total_requests": total_requests,
            "avg_latency": round(avg_latency, 2),
            "error_rate": round(error_rate * 100, 2),  # Convert to percentage
            "p95_latency": round(p95_latency, 2)
        })

    # Step 4: Sort and return top 5 by error rate
    results.sort(key=lambda x: (-x["error_rate"], -x["total_requests"]))
    return results[:5]