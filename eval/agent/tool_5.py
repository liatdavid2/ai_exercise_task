import glob
import os
import re
from collections import defaultdict

def tool():
    # Step 1: Discover files
    files = glob.glob('data/**/*.log', recursive=True) + glob.glob('**/*.log', recursive=True)
    files = list(set(files))  # Remove duplicates

    # Prefer files inside 'data/' if exist
    if not files:
        # Fallback to known filenames
        known_files = ['employees.json', 'sales.csv', 'app.log']
        for known_file in known_files:
            if os.path.exists(known_file):
                files.append(known_file)

        # Try os.walk('data/')
        for root, _, filenames in os.walk('data/'):
            for filename in filenames:
                if filename.endswith('.log'):
                    files.append(os.path.join(root, filename))

    if not files:
        return "No matching file found"

    # Step 2: Initialize data structures
    log_data = defaultdict(list)

    # Step 3: Read and parse the log file
    for file in files:
        if not file.endswith('.log'):
            continue
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
                raw_latency = data.get("latency_ms", "0")
                m = re.search(r'(\d+\.?\d*)', raw_latency)
                latency = float(m.group(1)) if m else 0

                # Fallback for missing endpoint
                if not endpoint:
                    m = re.search(r'/api/\S+', line)
                    endpoint = m.group(0) if m else None

                if endpoint:
                    log_data[(method, endpoint)].append((status, latency))

    # Step 4: Compute metrics
    results = []
    for (method, endpoint), entries in log_data.items():
        total_requests = len(entries)
        latencies = [latency for _, latency in entries]
        avg_latency = sum(latencies) / total_requests if total_requests > 0 else 0
        error_count = sum(1 for status, _ in entries if status >= 400)
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

    # Step 5: Sort results by error rate descending and get top 5
    results.sort(key=lambda x: x['error_rate'], reverse=True)
    top_results = results[:5]

    return top_results

# Example usage
if __name__ == "__main__":
    output = tool()
    for entry in output:
        print(entry)