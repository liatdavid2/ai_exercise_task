import glob
import os
import re
from collections import defaultdict

def tool():
    # Step 1: File Discovery
    log_files = glob.glob('data/**/*.log', recursive=True) + glob.glob('**/*.log', recursive=True)
    log_files = list(set(log_files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    log_files = sorted(log_files, key=lambda x: 0 if x.startswith('data/') else 1)

    if not log_files:
        # Fallback search
        known_files = ['app.log']
        for filename in known_files:
            if os.path.exists(filename):
                log_files.append(filename)
                break

        if not log_files:
            # Try os.walk
            for root, dirs, files in os.walk('data/'):
                for file in files:
                    if file.endswith('.log'):
                        log_files.append(os.path.join(root, file))
                        break

    if not log_files:
        return "No matching file found"

    # Step 2: Log Analysis
    method_endpoint_stats = defaultdict(lambda: {
        "total_requests": 0,
        "latencies": [],
        "error_count": 0
    })

    for log_file in log_files:
        with open(log_file, 'r') as f:
            for line in f:
                # Parse the log line
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

                # Extract latency
                raw_latency = data.get("latency_ms", "0")
                m = re.search(r'(\d+\.?\d*)', raw_latency)
                latency = float(m.group(1)) if m else 0

                # Fallback for missing endpoint
                if not endpoint:
                    m = re.search(r'/api/\S+', line)
                    endpoint = m.group(0) if m else None

                if not endpoint:
                    continue  # Skip line if endpoint is still missing

                # Grouping
                key = (method, endpoint)
                method_endpoint_stats[key]["total_requests"] += 1
                method_endpoint_stats[key]["latencies"].append(latency)
                if status >= 400:
                    method_endpoint_stats[key]["error_count"] += 1

    # Step 3: Calculate statistics
    results = []
    for (method, endpoint), stats in method_endpoint_stats.items():
        total_requests = stats["total_requests"]
        latencies = stats["latencies"]
        error_count = stats["error_count"]

        avg_latency = sum(latencies) / total_requests
        error_rate = error_count / total_requests
        latencies_sorted = sorted(latencies)
        p95_index = int(0.95 * (len(latencies_sorted) - 1))
        p95_latency = latencies_sorted[p95_index]

        results.append({
            "method": method,
            "endpoint": endpoint,
            "total_requests": total_requests,
            "avg_latency": avg_latency,
            "error_rate": error_rate,
            "p95_latency": p95_latency
        })

    # Step 4: Sort by error_rate DESC and return top 5
    results.sort(key=lambda x: x["error_rate"], reverse=True)
    return results[:5]

# Example usage
if __name__ == "__main__":
    top_endpoints = tool()
    for entry in top_endpoints:
        print(entry)