import glob
import re
from collections import defaultdict

def tool():
    # Step 1: Discover relevant files
    files = glob.glob('data/**/*.log', recursive=True) + glob.glob('**/*.log', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Fallback if no files found
    if not files:
        known_files = ['employees.json', 'sales.csv', 'app.log']
        for known_file in known_files:
            if glob.glob(known_file):
                files = glob.glob(known_file)
                break

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
                raw_latency = data.get("latency_ms", "0")
                m = re.search(r'(\d+\.?\d*)', raw_latency)
                latency = float(m.group(1)) if m else 0

                if endpoint is None:
                    m = re.search(r'/api/\S+', line)
                    endpoint = m.group(0) if m else None

                if endpoint is not None:
                    log_data[(method, endpoint)].append((status, latency))

    # Step 5: Grouping and calculations
    results = []
    for (method, endpoint), entries in log_data.items():
        total_requests = len(entries)
        latencies = [latency for _, latency in entries]
        error_count = sum(1 for status, _ in entries if status >= 400)
        avg_latency = sum(latencies) / total_requests if total_requests > 0 else 0
        error_rate = (error_count / total_requests) * 100 if total_requests > 0 else 0
        p95_latency = sorted(latencies)[int(0.95 * (len(latencies) - 1))] if latencies else 0

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