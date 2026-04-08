import glob
import re
from collections import defaultdict
import os

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
        files = list(set(files))  # Remove duplicates again

    if not files:
        return "No matching file found"

    # Step 2: Parse log files
    log_data = defaultdict(lambda: {'count': 0, 'latencies': [], 'errors': 0})

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

                # Fallback for endpoint if missing
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
                log_data[key]['count'] += 1
                log_data[key]['latencies'].append(latency)
                if status >= 400:
                    log_data[key]['errors'] += 1

    # Step 3: Compute metrics
    results = []
    for (method, endpoint), metrics in log_data.items():
        total_requests = metrics['count']
        avg_latency = sum(metrics['latencies']) / total_requests
        error_rate = metrics['errors'] / total_requests
        latencies_sorted = sorted(metrics['latencies'])
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
    results.sort(key=lambda x: x['error_rate'], reverse=True)
    return results[:5]

# Example usage
if __name__ == "__main__":
    top_endpoints = tool()
    for entry in top_endpoints:
        print(entry)