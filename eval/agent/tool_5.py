import glob
import re
from collections import defaultdict

def tool():
    # Step 1: File Discovery
    files = glob.glob('data/**/*.log', recursive=True) + glob.glob('**/*.log', recursive=True)
    files = list(set(files))  # Remove duplicates
    files = [f for f in files if 'data/' in f] or files  # Prefer files inside 'data/' if exist

    if not files:
        # Fallback search
        files = glob.glob('employees.json') + glob.glob('sales.csv') + glob.glob('app.log')
        if not files:
            import os
            for root, dirs, filenames in os.walk('data/'):
                for filename in filenames:
                    if filename.endswith('.log'):
                        files.append(os.path.join(root, filename))
            if not files:
                return "No matching file found"

    # Step 2: Parse Log File
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

                # Step 3: Extract Fields
                method = data.get("method", "GET")
                endpoint = data.get("endpoint")
                status = int(data.get("status", 200))

                # Fallback if endpoint is missing
                if not endpoint:
                    m = re.search(r'/api/\S+', line)
                    endpoint = m.group(0) if m else None
                    if not endpoint:
                        continue  # Skip line if endpoint is still missing

                # Extract latency
                raw = data.get("latency_ms", "0")
                m = re.search(r'(\d+\.?\d*)', raw)
                latency = float(m.group(1)) if m else 0

                # Step 4: Grouping
                key = (method, endpoint)
                log_data[key]['count'] += 1
                log_data[key]['latencies'].append(latency)
                if status >= 400:
                    log_data[key]['errors'] += 1

    # Step 5: Calculate Metrics
    results = []
    for (method, endpoint), metrics in log_data.items():
        total_requests = metrics['count']
        avg_latency = sum(metrics['latencies']) / total_requests
        error_rate = metrics['errors'] / total_requests
        latencies_sorted = sorted(metrics['latencies'])
        index = int(0.95 * (len(latencies_sorted) - 1))
        p95_latency = latencies_sorted[index]

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

    # Step 7: Output Top 5
    return results[:5]