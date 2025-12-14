#!/usr/bin/env python3
"""
Single MongoDB Instance Experiment
Measures: Insert throughput, Read QPS, Update ops/s
"""
import sys
import time
import random
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

def wait_for_connection(uri, max_retries=30):
    """Wait for MongoDB to be ready"""
    for i in range(max_retries):
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=2000)
            client.admin.command('ping')
            print(f"✓ Connected to MongoDB")
            return client
        except ConnectionFailure:
            print(f"Waiting for MongoDB... ({i+1}/{max_retries})")
            time.sleep(2)
    raise Exception("Could not connect to MongoDB")

def main():
    uri = sys.argv[1] if len(sys.argv) > 1 else 'mongodb://localhost:27017'
    print(f"\n{'='*70}")
    print("SINGLE MONGODB INSTANCE - PERFORMANCE BASELINE")
    print(f"{'='*70}")
    print(f"Architecture: Single mongod process")
    print(f"URI: {uri}")
    print(f"Dataset: 100,000 documents (simulating big data workload)")
    print(f"{'='*70}\n")
    
    client = wait_for_connection(uri)
    db = client['analyticsDB']
    col = db['events']
    
    # Clean slate
    col.drop()
    print("✓ Collection cleaned\n")
    
    # ===== INSERT TEST =====
    N = 100000  # Big data volume
    print(f"[1/4] INSERT TEST: {N:,} documents")
    print("-" * 70)
    print("Simulating: Real-time event streaming (clicks, purchases, views)")
    
    batch_size = 1000
    start = time.time()
    total_docs = 0
    
    for batch_start in range(0, N, batch_size):
        docs = []
        for i in range(batch_start, min(batch_start + batch_size, N)):
            docs.append({
                'userId': i % 1000,  # 1000 unique users
                'eventType': random.choice(['click', 'view', 'purchase', 'search', 'cart_add']),
                'timestamp': time.time(),
                'value': random.randint(1, 5000),
                'sessionId': f"sess_{i % 500}",
                'metadata': {
                    'device': random.choice(['mobile', 'desktop', 'tablet']),
                    'country': random.choice(['US', 'UK', 'CA', 'AU', 'DE']),
                    'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge'])
                }
            })
        col.insert_many(docs, ordered=False)
        total_docs += len(docs)
        
        if (batch_start + batch_size) % 20000 == 0:
            elapsed = time.time() - start
            rate = total_docs / elapsed
            print(f"  Progress: {total_docs:>6,} docs | Current rate: {rate:>6,.0f} ops/sec")
    
    insert_time = time.time() - start
    insert_rate = N / insert_time
    print(f"\n{'─'*70}")
    print(f"✓ INSERT COMPLETED")
    print(f"  Time taken:       {insert_time:>10.2f} seconds")
    print(f"  Throughput:       {insert_rate:>10,.0f} ops/sec")
    print(f"  Total documents:  {col.count_documents({}):>10,}")
    print(f"  Bottleneck:       Single node disk I/O + CPU")
    print(f"{'─'*70}\n")
    
    # ===== READ TEST =====
    Q = 10000
    print(f"[2/4] READ TEST: {Q:,} targeted queries (by userId)")
    print("-" * 70)
    print("Simulating: User activity lookups, analytics dashboards")
    
    start = time.time()
    for i in range(Q):
        uid = random.randint(0, 999)
        list(col.find({'userId': uid}).limit(10))
        
        if (i + 1) % 2000 == 0:
            elapsed = time.time() - start
            rate = (i + 1) / elapsed
            print(f"  Progress: {i+1:>5,} queries | Current QPS: {rate:>6,.0f}")
    
    read_time = time.time() - start
    read_qps = Q / read_time
    print(f"\n{'─'*70}")
    print(f"✓ READ COMPLETED")
    print(f"  Time taken:       {read_time:>10.2f} seconds")
    print(f"  Query rate:       {read_qps:>10,.0f} queries/sec")
    print(f"  Avg latency:      {1000/read_qps:>10.2f} ms/query")
    print(f"  Note:             All queries scan single node")
    print(f"{'─'*70}\n")
    
    # ===== UPDATE TEST =====
    U = 10000
    print(f"[3/4] UPDATE TEST: {U:,} random updates")
    print("-" * 70)
    print("Simulating: User profile updates, transaction processing")
    
    start = time.time()
    for i in range(U):
        uid = random.randint(0, 999)
        col.update_one({'userId': uid}, {'$inc': {'value': 1}, '$set': {'lastUpdate': time.time()}})
        
        if (i + 1) % 2000 == 0:
            elapsed = time.time() - start
            rate = (i + 1) / elapsed
            print(f"  Progress: {i+1:>5,} updates | Current rate: {rate:>6,.0f} ops/sec")
    
    update_time = time.time() - start
    update_rate = U / update_time
    print(f"\n{'─'*70}")
    print(f"✓ UPDATE COMPLETED")
    print(f"  Time taken:       {update_time:>10.2f} seconds")
    print(f"  Update rate:      {update_rate:>10,.0f} ops/sec")
    print(f"  Bottleneck:       Single node write lock contention")
    print(f"{'─'*70}\n")
    
    # ===== AGGREGATION TEST =====
    print(f"[4/4] AGGREGATION TEST: Complex analytics query")
    print("-" * 70)
    print("Simulating: Business intelligence reporting")
    
    start = time.time()
    result = list(col.aggregate([
        {'$group': {
            '_id': {'type': '$eventType', 'device': '$metadata.device'},
            'count': {'$sum': 1},
            'avgValue': {'$avg': '$value'},
            'totalValue': {'$sum': '$value'}
        }},
        {'$sort': {'count': -1}},
        {'$limit': 20}
    ]))
    agg_time = time.time() - start
    
    print(f"  Computed: Event type x Device breakdown")
    print(f"  Records processed: {N:,}")
    print(f"  Results returned:  {len(result)}")
    print(f"\n{'─'*70}")
    print(f"✓ AGGREGATION COMPLETED")
    print(f"  Time taken:       {agg_time:>10.2f} seconds")
    print(f"  Processing rate:  {N/agg_time:>10,.0f} docs/sec")
    print(f"  Note:             Single CPU core bottleneck")
    print(f"{'─'*70}\n")
    
    # ===== FINAL SUMMARY =====
    print(f"\n{'='*70}")
    print("SINGLE INSTANCE - FINAL RESULTS")
    print(f"{'='*70}")
    print(f"{'Operation':<20} {'Throughput':>20} {'Time (sec)':>15}")
    print(f"{'-'*70}")
    print(f"{'Insert':<20} {insert_rate:>17,.0f} ops/sec {insert_time:>12.2f}")
    print(f"{'Read':<20} {read_qps:>17,.0f} qps {read_time:>12.2f}")
    print(f"{'Update':<20} {update_rate:>17,.0f} ops/sec {update_time:>12.2f}")
    print(f"{'Aggregation':<20} {N/agg_time:>17,.0f} docs/sec {agg_time:>12.2f}")
    print(f"{'='*70}")
    print(f"\n📊 LIMITATIONS OF SINGLE NODE:")
    print(f"  • Single point of failure (no redundancy)")
    print(f"  • Limited by single machine CPU/RAM/Disk")
    print(f"  • No horizontal scalability")
    print(f"  • Write contention on single mongod process")
    print(f"  • All data on one disk (I/O bottleneck)")
    print(f"\n💡 EXPECTATION: Cluster should improve throughput by")
    print(f"   distributing load across multiple shards.\n")
    print(f"{'='*70}\n")
    
    client.close()

if __name__ == '__main__':
    main()