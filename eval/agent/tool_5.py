import glob
import re
from collections import defaultdict

def tool():
    # Step 1: Discover the relevant log file
    files = glob.glob('data/**/*.log', recursive=True) + glob.glob('**/*.log', recursive=True)
    files = list(set(files))  # Remove duplicates
    log_file = None

    # Prefer files inside 'data/' if exist
    for f in files:
        if 'data/' in f:
            log_file = f
            break
    if not log_file and files:
        log_file = files[0]

    if not log_file:
        # Fallback to known filenames
        known_files = ['app.log']
        for known_file in known_files:
            if known_file in files:
                log_file = known_file
                break

    if not log_file:
        return "No matching file found"

    # Step 2: Parse the log file
    endpoint_data = defaultdict(lambda: {'count': 0, 'latencies': [], 'errors': 0})

    with open(log_file, 'r') as f:
        for line in f:
            parts = line.strip().split()
            data = {}
            for p in parts:
                if '=' in p:
                    k, v = p.split('=', 1)
                    data[k] = v

            # Step 3: Extract fields
            method = data.get("method", "GET")
            endpoint = data.get("endpoint")
            status = int(data.get("status", 200))

            # For latency
            raw = data.get("latency_ms", "0")
            m = re.search(r'(\d+\.?\d*)', raw)
            latency = float(m.group(1)) if m else 0

            # Fallback if endpoint is missing
            if not endpoint:
                m = re.search(r'/api/\S+', line)
                endpoint = m.group(0) if m else None

            if not endpoint:
                continue  # Skip line if endpoint is still missing

            # Step 4: Grouping
            key = (method, endpoint)
            endpoint_data[key]['count'] += 1
            endpoint_data[key]['latencies'].append(latency)
            if status >= 400:
                endpoint_data[key]['errors'] += 1

    # Step 5: Calculate metrics
    results = []
    for (method, endpoint), data in endpoint_data.items():
        total_requests = data['count']
        avg_latency = sum(data['latencies']) / total_requests
        error_rate = data['errors'] / total_requests
        values = sorted(data['latencies'])
        index = int(0.95 * (len(values) - 1))
        p95_latency = values[index]

        results.append({
            "method": method,
            "endpoint": endpoint,
            "total_requests": total_requests,
            "avg_latency": avg_latency,
            "error_rate": error_rate,
            "p95_latency": p95_latency
        })

    # Step 6: Sort by error_rate DESC
    results.sort(key=lambda x: x['error_rate'], reverse=True)

    # Step 7: Output top 5 groups
    return results[:5]