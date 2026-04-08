import glob
import os
import re
from collections import defaultdict

def tool():
    # Step 1: File Discovery
    files = glob.glob('data/**/*.log', recursive=True) + glob.glob('**/*.log', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    files = sorted(files, key=lambda x: (not x.startswith('data/'), x))

    if not files:
        # Fallback: Try known filenames
        known_files = ['app.log']
        for filename in known_files:
            if os.path.exists(filename):
                files.append(filename)

    if not files:
        # Fallback: Try os.walk
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.log'):
                    files.append(os.path.join(root, filename))

    if not files:
        return "No matching file found"

    # Step 2: Parse and Analyze Logs
    log_data = defaultdict(lambda: {'latencies': [], 'total_requests': 0, 'error_count': 0})

    for file in files:
        with open(file, 'r') as f:
            for line in f:
                parts = line.strip().split()
                data = {}
                for p in parts:
                    if '=' in p:
                        k, v = p.split('=', 1)
                        data[k] = v

                # Extract fields
                method = data.get("method", "GET")
                endpoint = data.get("endpoint")
                status = int(data.get("status", 200))

                # Fallback for missing endpoint
                if not endpoint:
                    m = re.search(r'/api/\S+', line)
                    endpoint = m.group(0) if m else None

                if not endpoint:
                    continue  # Skip line if endpoint is still missing

                # Extract latency
                raw = data.get("latency_ms", "0")
                m = re.search(r'(\d+\.?\d*)', raw)
                latency = float(m.group(1)) if m else 0

                # Group by (method, endpoint)
                key = (method, endpoint)
                log_data[key]['latencies'].append(latency)
                log_data[key]['total_requests'] += 1
                if status >= 400:
                    log_data[key]['error_count'] += 1

    # Step 3: Calculate Statistics
    results = []
    for (method, endpoint), stats in log_data.items():
        total_requests = stats['total_requests']
        latencies = stats['latencies']
        avg_latency = sum(latencies) / total_requests
        error_rate = stats['error_count'] / total_requests
        values = sorted(latencies)
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

    # Step 4: Sort and Return Top 5
    results.sort(key=lambda x: x['error_rate'], reverse=True)
    return results[:5]

# Example usage
if __name__ == "__main__":
    top_endpoints = tool()
    for entry in top_endpoints:
        print(entry)