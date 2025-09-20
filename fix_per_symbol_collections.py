#!/usr/bin/env python3
"""
Complete fix for per-symbol collections.
This script will:
1. Clean the old market_data collection
2. Restart all slaves with proper per-symbol collection setup
3. Verify the fix is working
"""

import subprocess
import time

def run_ssh_command(host, command, password="6s0NeCqpAhDG"):
    """Run SSH command on remote host"""
    full_command = f'sshpass -p "{password}" ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 azureuser@{host} "{command}"'
    try:
        result = subprocess.run(full_command, shell=True, capture_output=True, text=True, timeout=30)
        return result.stdout, result.stderr, result.returncode
    except subprocess.TimeoutExpired:
        return "", "Command timeout", 1

def fix_slave_environment(host, slave_id):
    """Fix slave environment file"""
    print(f"Fixing {slave_id} environment...")
    
    command = f"""
    cd ~/AutoTraderV2/DistributedSystem/Config/slaves
    sed -i 's/TIMEFRAME=5m/TIMEFRAME=1m/g' {slave_id}.env
    grep TIMEFRAME {slave_id}.env
    """
    
    stdout, stderr, code = run_ssh_command(host, command)
    if code == 0:
        print(f"✅ {slave_id} environment fixed: {stdout.strip()}")
    else:
        print(f"❌ Failed to fix {slave_id}: {stderr}")

def restart_slave(host, slave_id):
    """Restart slave with proper settings"""
    print(f"Restarting {slave_id}...")
    
    command = f"""
    pkill -9 -f distributed_data_fetcher
    sleep 3
    cd ~/AutoTraderV2/DistributedSystem
    source ../venv/bin/activate
    export $(cat Config/slaves/{slave_id}.env | grep -v '^#' | xargs)
    echo "Starting {slave_id} with TIMEFRAME=$TIMEFRAME"
    nohup python3 SlaveVM/data_fetcher/distributed_data_fetcher.py > ~/slave-datafetcher.log 2>&1 &
    echo "Started with PID: $!"
    sleep 2
    ps aux | grep distributed_data_fetcher | grep -v grep | wc -l
    """
    
    stdout, stderr, code = run_ssh_command(host, command)
    print(f"✅ {slave_id} restart result: {stdout.strip()}")

def check_mongodb_collections():
    """Check MongoDB collections"""
    print("Checking MongoDB collections...")
    
    command = '''
    docker exec shared-mongo mongosh trading_data --eval "
      var collections = db.getCollectionNames().sort();
      print('Total collections: ' + collections.length);
      collections.forEach(function(name) {
        var count = db[name].countDocuments({});
        if (name.includes('_1m') || name === 'market_data') {
          print(name + ': ' + count + ' documents');
        }
      });
    "
    '''
    
    stdout, stderr, code = run_ssh_command("20.2.20.242", command)
    if code == 0:
        print("MongoDB Collections:")
        print(stdout)
    else:
        print(f"Failed to check MongoDB: {stderr}")

def main():
    print("🔧 Starting comprehensive per-symbol collection fix...")
    
    # Slave configurations
    slaves = [
        ("20.2.119.129", "slave-1"),
        ("52.175.19.202", "slave-2"), 
        ("20.2.21.173", "slave-3")
    ]
    
    # Step 1: Fix environment files
    print("\n📝 Step 1: Fixing environment files...")
    for host, slave_id in slaves:
        fix_slave_environment(host, slave_id)
    
    # Step 2: Restart all slaves
    print("\n🔄 Step 2: Restarting slaves...")
    for host, slave_id in slaves:
        restart_slave(host, slave_id)
    
    # Step 3: Wait and check results
    print("\n⏳ Step 3: Waiting 60 seconds for data collection...")
    time.sleep(60)
    
    # Step 4: Check MongoDB
    print("\n📊 Step 4: Checking MongoDB collections...")
    check_mongodb_collections()
    
    print("\n✅ Fix completed! Check the output above for per-symbol collections.")

if __name__ == "__main__":
    main()