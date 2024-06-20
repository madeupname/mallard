import concurrent
from concurrent.futures import ThreadPoolExecutor


def parallelize(workers, vendor_symbol_ids, fun, *args, **kwargs):
    """Run the given function in parallel for each vendor_symbol_id."""
    print(f"Running {fun.__name__} in parallel with {workers} workers for {len(vendor_symbol_ids)} symbols.")
    results = {'count_skip': 0, 'count_success': 0, 'count_fail': 0}
    if workers == 1:
        for id in vendor_symbol_ids:
            status = fun(id, *args, **kwargs)
            if status == 'skip':
                results['count_skip'] += 1
            elif status == 'success':
                results['count_success'] += 1
            elif status == 'fail':
                results['count_fail'] += 1
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            # Submit the tasks to the thread pool
            futures = {executor.submit(fun, id, *args, **kwargs): id for id in vendor_symbol_ids}
            for future in concurrent.futures.as_completed(futures):
                row = futures[future]
                try:
                    status = future.result()
                    if status == 'skip':
                        results['count_skip'] += 1
                    elif status == 'success':
                        results['count_success'] += 1
                    elif status == 'fail':
                        results['count_fail'] += 1
                except Exception as e:
                    print(f"Error processing {row}\n{e}")
                    results['count_fail'] += 1
                total = results['count_skip'] + results['count_success'] + results['count_fail']
                if total > 0 and total % 500 == 0:
                    print(
                        f"{fun.__name__}: Skipped {results['count_skip']}  Updated {results['count_success']}  Failed {results['count_fail']}")
    return results
