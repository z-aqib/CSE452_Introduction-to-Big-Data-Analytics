#!/usr/bin/env python3
import sys
import time
import random
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

def wait_for_connection(uri, max_retries=60):
    for i in range(max_retries):
        try:
            # Connect directly, don't try to use replica set
            client = MongoClient(uri, serverSelectionTimeoutMS=5000, connectTimeoutMS=5000, directConnection=True)
            client.admin.command('ping')
            if i > 0:
                print(f"  ✓ Connected to {uri}")
            return client
        except Exception as e:
            if i % 10 == 0:
                print(f"  Waiting for {uri}... attempt {i+1}/{max_retries}")
            time.sleep(2)
    raise Exception(f"Could not connect to {uri} after {max_retries} attempts")

def init_replica_set(host, port, replset_name, members):
    uri = f'mongodb://{host}:{port}'
    print(f"  Connecting to {uri}...")
    client = wait_for_connection(uri)
    
    try:
        config = {
            '_id': replset_name,
            'members': [{'_id': i, 'host': m} for i, m in enumerate(members)]
        }
        client.admin.command('replSetInitiate', config)
        print(f"  ✓ Initiated replica set: {replset_name}")
        return True
    except OperationFailure as e:
        if 'already initialized' in str(e):
            print(f"  ✓ Replica set {replset_name} already initialized")
            return True
        else:
            print(f"  ✗ Error: {e}")
            return False
    finally:
        client.close()

def wait_for_primary(host, port, max_wait=30):
    uri = f'mongodb://{host}:{port}'
    client = MongoClient(uri, serverSelectionTimeoutMS=2000)
    
    for i in range(max_wait):
        try:
            status = client.admin.command('replSetGetStatus')
            for member in status['members']:
                if member['stateStr'] == 'PRIMARY':
                    print(f"  ✓ Primary elected in replica set")
                    client.close()
                    return True
        except:
            pass
        time.sleep(1)
    
    client.close()
    return False

def main():
    MONGOS_URI = sys.argv[1] if len(sys.argv) > 1 else 'mongodb://localhost:27017'
    
    print(f"\n{'='*70}")
    print("MONGODB SHARDED CLUSTER - SETUP AND PERFORMANCE TEST")
    print(f"{'='*70}\n")
    
    print("[1/5] Initializing Config Server Replica Set (3 nodes)")
    print("-" * 70)
    init_replica_set('cfg1', 27019, 'cfgRepl', 
                     ['cfg1:27019', 'cfg2:27019', 'cfg3:27019'])
    wait_for_primary('cfg1', 27019)
    print()
    
    print("[2/5] Initializing Shard A Replica Set (3 nodes)")
    print("-" * 70)
    init_replica_set('shardA1', 27018, 'shardA',
                     ['shardA1:27018', 'shardA2:27018', 'shardA3:27018'])
    wait_for_primary('shardA1', 27018)
    print()
    
    print("[3/5] Initializing Shard B Replica Set (3 nodes)")
    print("-" * 70)
    init_replica_set('shardB1', 27018, 'shardB',
                     ['shardB1:27018', 'shardB2:27018', 'shardB3:27018'])
    wait_for_primary('shardB1', 27018)
    print()
    
    print("[4/5] Adding Shards to Cluster via Mongos Router")
    print("-" * 70)
    print(f"  Connecting to mongos: {MONGOS_URI}")
    mongos = wait_for_connection(MONGOS_URI)
    
    for shard_name, shard_uri in [
        ('shardA', 'shardA/shardA1:27018,shardA2:27018,shardA3:27018'),
        ('shardB', 'shardB/shardB1:27018,shardB2:27018,shardB3:27018')
    ]:
        try:
            mongos.admin.command('addShard', shard_uri, name=shard_name)
            print(f"  ✓ Added {shard_name} to cluster")
        except OperationFailure as e:
            if 'already exists' in str(e) or 'already' in str(e):
                print(f"  ✓ Shard {shard_name} already exists")
    
    print()
    
    print("[5/5] Configuring Database Sharding")
    print("-" * 70)
    
    try:
        mongos.admin.command('enableSharding', 'analyticsDB')
        print("  ✓ Enabled sharding on database: analyticsDB")
    except OperationFailure as e:
        if 'already' in str(e).lower():
            print("  ✓ Sharding already enabled")
    
    try:
        mongos.admin.command('shardCollection', 'analyticsDB.events', 
                            key={'userId': 'hashed'})
        print("  ✓ Sharded collection on userId (hashed)")
    except OperationFailure as e:
        if 'already' in str(e).lower():
            print("  ✓ Collection already sharded")
    
    print("\n✓ Cluster setup complete!\n")
    time.sleep(3)
    
    print(f"{'='*70}")
    print("SHARDED CLUSTER - PERFORMANCE TEST")
    print(f"{'='*70}\n")
    
    db = mongos['analyticsDB']
    col = db['events']
    col.drop()
    print("✓ Collection cleaned\n")
    
    # INSERT TEST
    N = 100000
    print(f"[1/4] INSERT TEST: {N:,} documents")
    print("-" * 70)
    
    batch_size = 1000
    start = time.time()
    total_docs = 0
    
    for batch_start in range(0, N, batch_size):
        docs = []
        for i in range(batch_start, min(batch_start + batch_size, N)):
            docs.append({
                'userId': i % 1000,
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
            print(f"  Progress: {total_docs:>6,} docs | Rate: {rate:>6,.0f} ops/sec")
    
    insert_time = time.time() - start
    insert_rate = N / insert_time
    print(f"\n✓ INSERT COMPLETED")
    print(f"  Time: {insert_time:.2f}s | Throughput: {insert_rate:,.0f} ops/sec\n")
    
    # Check distribution
    time.sleep(2)
    print("📊 DATA DISTRIBUTION:")
    print("-" * 70)
    try:
        shard_dist = mongos.admin.command('collStats', 'analyticsDB.events', verbose=True)
        shards = shard_dist.get('shards', {})
        for shard_name, stats in shards.items():
            count = stats.get('count', 0)
            pct = (100 * count / N) if N > 0 else 0
            print(f"  {shard_name}: {count:,} docs ({pct:.1f}%)")
        print(f"  TOTAL: {col.count_documents({}):,} docs")
    except:
        print(f"  TOTAL: {col.count_documents({}):,} docs")
    print()
    
    # READ TEST
    Q = 10000
    print(f"[2/4] READ TEST: {Q:,} queries")
    print("-" * 70)
    
    start = time.time()
    for i in range(Q):
        uid = random.randint(0, 999)
        list(col.find({'userId': uid}).limit(10))
        
        if (i + 1) % 2000 == 0:
            elapsed = time.time() - start
            rate = (i + 1) / elapsed
            print(f"  Progress: {i+1:>5,} queries | QPS: {rate:>6,.0f}")
    
    read_time = time.time() - start
    read_qps = Q / read_time
    print(f"\n✓ READ COMPLETED")
    print(f"  Time: {read_time:.2f}s | QPS: {read_qps:,.0f}\n")
    
    # UPDATE TEST
    U = 10000
    print(f"[3/4] UPDATE TEST: {U:,} updates")
    print("-" * 70)
    
    start = time.time()
    for i in range(U):
        uid = random.randint(0, 999)
        col.update_one({'userId': uid}, {'$inc': {'value': 1}})
        
        if (i + 1) % 2000 == 0:
            elapsed = time.time() - start
            rate = (i + 1) / elapsed
            print(f"  Progress: {i+1:>5,} updates | Rate: {rate:>6,.0f} ops/sec")
    
    update_time = time.time() - start
    update_rate = U / update_time
    print(f"\n✓ UPDATE COMPLETED")
    print(f"  Time: {update_time:.2f}s | Throughput: {update_rate:,.0f} ops/sec\n")
    
    # AGGREGATION TEST
    print(f"[4/4] AGGREGATION TEST")
    print("-" * 70)
    
    start = time.time()
    result = list(col.aggregate([
        {'$group': {
            '_id': {'type': '$eventType', 'device': '$metadata.device'},
            'count': {'$sum': 1},
            'avgValue': {'$avg': '$value'}
        }},
        {'$sort': {'count': -1}},
        {'$limit': 20}
    ]))
    agg_time = time.time() - start
    
    print(f"  Processed: {N:,} docs")
    print(f"  Results: {len(result)} groups")
    print(f"\n✓ AGGREGATION COMPLETED")
    print(f"  Time: {agg_time:.2f}s | Rate: {N/agg_time:,.0f} docs/sec\n")
    
    # SUMMARY
    print(f"{'='*70}")
    print("CLUSTER RESULTS SUMMARY")
    print(f"{'='*70}")
    print(f"Insert:      {insert_rate:>10,.0f} ops/sec")
    print(f"Read:        {read_qps:>10,.0f} queries/sec")
    print(f"Update:      {update_rate:>10,.0f} ops/sec")
    print(f"Aggregation: {N/agg_time:>10,.0f} docs/sec")
    print(f"{'='*70}")
    print("\n🚀 BENEFITS: Parallel processing, load distribution, high availability")
    print("📊 Compare with single instance: docker logs init-single\n")
    
    mongos.close()

if __name__ == '__main__':
    main()
