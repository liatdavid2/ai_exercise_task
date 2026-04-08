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
    files = [f for f in files if 'data/' in f] or files  # Prefer files inside 'data/' if exist

    if not files:
        # Fallback to os.walk if no files found
        for root, dirs, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.log'):
                    files.append(os.path.join(root, filename))
        if not files:
            return "No matching file found"

    # Step 2: Parse log files
    method_key_options = ["method", "http_method", "verb"]
    endpoint_key_options = ["endpoint", "path", "route", "url", "uri"]
    status_key_options = ["status", "status_code", "code", "response_code"]
    latency_key_options = ["latency_ms", "latency", "response_time_ms", "response_time", "duration_ms", "duration"]

    endpoint_data = defaultdict(lambda: {"total_requests": 0, "latencies": [], "errors": 0})

    for file in files:
        with open(file, 'r') as f:
            for line in f:
                parts = line.strip().split()
                data = {}
                for p in parts:
                    if '=' in p:
                        k, v = p.split('=', 1)
                        data[k] = v

                # Extract core fields
                method = data.get(find_key(data, method_key_options))
                endpoint = data.get(find_key(data, endpoint_key_options))
                status = data.get(find_key(data, status_key_options))
                raw_latency = data.get(find_key(data, latency_key_options))

                # Safe parsing
                if not endpoint:
                    # Fallback to regex extraction for endpoint
                    match = re.search(r'/api/\S*', line)
                    if match:
                        endpoint = match.group(0)
                    else:
                        continue  # Skip if endpoint is still missing

                if not method or not status:
                    continue  # Skip if method or status is missing

                try:
                    status = int(status)
                except ValueError:
                    continue  # Skip if status is not a valid integer

                # Parse latency
                latency = None
                if raw_latency:
                    raw_latency = str(raw_latency).strip()
                    m = re.search(r'(\d+(?:\.\d+)?)', raw_latency)
                    if m:
                        latency = float(m.group(1))

                # Grouping
                key = (method, endpoint)
                endpoint_data[key]["total_requests"] += 1
                if latency is not None:
                    endpoint_data[key]["latencies"].append(latency)
                if status >= 400:
                    endpoint_data[key]["errors"] += 1

    # Step 3: Compute metrics
    results = []
    for (method, endpoint), data in endpoint_data.items():
        total_requests = data["total_requests"]
        latencies = data["latencies"]
        errors = data["errors"]

        avg_latency = sum(latencies) / len(latencies) if latencies else 0
        error_rate = (errors / total_requests) * 100 if total_requests else 0

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

    # Step 4: Sort and return top 5
    results.sort(key=lambda x: (-x["error_rate"], -x["total_requests"]))
    return results[:5]

# Example usage
if __name__ == "__main__":
    top_endpoints = tool()
    for entry in top_endpoints:
        print(entry)