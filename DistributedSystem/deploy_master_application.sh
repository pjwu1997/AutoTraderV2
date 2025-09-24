#!/bin/bash
# Deploy Master VM Application (MongoDB + Master API + Symbol Distribution)

set -e

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${GREEN}🚀 MASTER VM APPLICATION DEPLOYMENT${NC}"
echo "====================================="
echo ""

# Check if deployment.env exists
if [ ! -f "deployment.env" ]; then
    echo -e "${RED}❌ deployment.env not found!${NC}"
    echo "Please run ./setup_env_simple.sh first"
    exit 1
fi

# Load configuration
source deployment.env

# Master VM IP from deployment
MASTER_IP="20.255.100.73"
USERNAME="$VM_ADMIN_USERNAME"
PASSWORD="$VM_ADMIN_PASSWORD"

echo -e "${BLUE}📋 Deployment Configuration:${NC}"
echo "   Master VM IP: $MASTER_IP"
echo "   Username: $USERNAME"
echo "   MongoDB Database: $MONGO_DB_NAME"
echo "   MongoDB User: $MONGO_USERNAME"
echo ""

# Check sshpass
if ! command -v sshpass &> /dev/null; then
    echo -e "${YELLOW}Installing sshpass...${NC}"
    if [[ "$OSTYPE" == "darwin"* ]]; then
        brew install hudochenkov/sshpass/sshpass
    else
        sudo apt-get update && sudo apt-get install -y sshpass
    fi
fi

# SSH command wrapper
ssh_exec() {
    local cmd="$1"
    sshpass -p "$PASSWORD" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "$USERNAME@$MASTER_IP" "$cmd"
}

# SCP file transfer wrapper
scp_file() {
    local local_file="$1"
    local remote_path="$2"
    sshpass -p "$PASSWORD" scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "$local_file" "$USERNAME@$MASTER_IP:$remote_path"
}

echo -e "${BLUE}🔄 Step 1: System Setup & Updates${NC}"
echo "--------------------------------"

# Update system and install dependencies
ssh_exec "sudo apt-get update -y"
ssh_exec "sudo apt-get install -y curl wget gnupg python3 python3-pip git docker.io docker-compose"
ssh_exec "sudo systemctl enable docker && sudo systemctl start docker"
ssh_exec "sudo usermod -aG docker $USERNAME"

echo "✅ System updated and Docker installed"
echo ""

echo -e "${BLUE}🍃 Step 2: MongoDB Installation${NC}"
echo "------------------------------"

# Install MongoDB
ssh_exec "curl -fsSL https://pgp.mongodb.com/server-7.0.asc | sudo gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg --dearmor"
ssh_exec "echo 'deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse' | sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list"
ssh_exec "sudo apt-get update -y"
ssh_exec "sudo apt-get install -y mongodb-org"

# Configure MongoDB
ssh_exec "sudo systemctl enable mongod"
ssh_exec "sudo systemctl start mongod"

# Wait for MongoDB to start
echo "Waiting for MongoDB to start..."
sleep 10

# Create MongoDB user and database
ssh_exec "mongosh --eval \"
use admin
db.createUser({
    user: '$MONGO_USERNAME',
    pwd: '$MONGO_PASSWORD',
    roles: [
        { role: 'userAdminAnyDatabase', db: 'admin' },
        { role: 'readWriteAnyDatabase', db: 'admin' },
        { role: 'dbAdminAnyDatabase', db: 'admin' }
    ]
})
use $MONGO_DB_NAME
db.createCollection('symbols')
db.createCollection('market_data')
exit
\""

echo "✅ MongoDB installed and configured"
echo ""

echo -e "${BLUE}🐍 Step 3: Python Environment Setup${NC}"
echo "-----------------------------------"

# Install Python packages
ssh_exec "pip3 install --upgrade pip"
ssh_exec "pip3 install pymongo flask flask-cors requests ccxt python-dotenv"

echo "✅ Python environment configured"
echo ""

echo -e "${BLUE}📁 Step 4: Create Directory Structure${NC}"
echo "------------------------------------"

# Create application directories
ssh_exec "mkdir -p /home/$USERNAME/autotrader/{master,logs,data,config}"
ssh_exec "mkdir -p /home/$USERNAME/autotrader/master/{api,symbol_distributor}"

echo "✅ Directory structure created"
echo ""

echo -e "${BLUE}📄 Step 5: Deploy Master Application Files${NC}"
echo "--------------------------------------------"

# Create symbol distribution script
cat > temp_full_symbol_distributor.py << 'EOF'
#!/usr/bin/env python3
"""
Full Symbol Distributor for Master VM
Distributes all Binance Perpetual symbols across slave VMs
"""

import os
import json
import ccxt
import math
from datetime import datetime
from pymongo import MongoClient

def get_all_binance_symbols():
    """Get all Binance Perpetual trading symbols"""
    try:
        exchange = ccxt.binance({
            'sandbox': False,
            'enableRateLimit': True,
            'options': {'defaultType': 'future'}
        })
        
        markets = exchange.load_markets()
        
        # Filter for USDT perpetual contracts
        perpetual_symbols = []
        for symbol, market in markets.items():
            if (market['type'] == 'swap' and 
                market['settle'] == 'USDT' and 
                market['active']):
                perpetual_symbols.append(symbol)
        
        print(f"✅ Found {len(perpetual_symbols)} active perpetual symbols")
        return sorted(perpetual_symbols)
        
    except Exception as e:
        print(f"❌ Error fetching symbols: {e}")
        return []

def distribute_symbols_to_slaves(symbols, num_slaves):
    """Distribute symbols evenly across slave VMs"""
    if not symbols:
        return {}
    
    symbols_per_slave = math.ceil(len(symbols) / num_slaves)
    distribution = {}
    
    for i in range(num_slaves):
        slave_id = f"slave-{i+1}"
        start_idx = i * symbols_per_slave
        end_idx = min(start_idx + symbols_per_slave, len(symbols))
        
        distribution[slave_id] = {
            'symbols': symbols[start_idx:end_idx],
            'count': len(symbols[start_idx:end_idx]),
            'slave_number': i + 1
        }
    
    return distribution

def save_distribution_to_mongodb(distribution):
    """Save symbol distribution to MongoDB"""
    try:
        mongo_uri = os.getenv('MONGO_URI', 'mongodb://trader:TradingData2025!@10.0.1.100:27017/trading_data')
        client = MongoClient(mongo_uri)
        db = client['trading_data']
        
        # Clear existing distribution
        db.symbol_distribution.delete_many({})
        
        # Save new distribution
        for slave_id, data in distribution.items():
            doc = {
                'slave_id': slave_id,
                'symbols': data['symbols'],
                'symbol_count': data['count'],
                'slave_number': data['slave_number'],
                'updated_at': datetime.utcnow(),
                'status': 'active'
            }
            db.symbol_distribution.insert_one(doc)
        
        print(f"✅ Distribution saved to MongoDB: {len(distribution)} slaves")
        return True
        
    except Exception as e:
        print(f"❌ Error saving to MongoDB: {e}")
        return False

def generate_slave_configs(distribution, master_ip):
    """Generate configuration files for each slave"""
    configs = {}
    
    for slave_id, data in distribution.items():
        config = {
            'SLAVE_ID': slave_id,
            'SYMBOLS': ','.join(data['symbols']),
            'MASTER_URL': f'http://{master_ip}:8080',
            'MONGO_URI': f'mongodb://trader:TradingData2025!@{master_ip}:27017/trading_data',
            'MONGO_DB_NAME': 'trading_data',
            'TIMEFRAME': '1m',
            'FETCH_INTERVAL': '60',
            'BATCH_SIZE': str(min(100, len(data['symbols'])))
        }
        configs[slave_id] = config
    
    return configs

def main():
    """Main distribution function"""
    print("🚀 FULL SYMBOL DISTRIBUTION")
    print("============================")
    
    # Get configuration from environment
    num_slaves = int(os.getenv('NUM_SLAVES', '5'))
    master_ip = os.getenv('MASTER_VM_IP', '10.0.1.100')
    
    print(f"📊 Configuration:")
    print(f"   Number of slaves: {num_slaves}")
    print(f"   Master IP: {master_ip}")
    print("")
    
    # Get all symbols
    print("📡 Fetching all Binance Perpetual symbols...")
    symbols = get_all_binance_symbols()
    
    if not symbols:
        print("❌ No symbols found!")
        return
    
    # Distribute symbols
    print(f"🔄 Distributing {len(symbols)} symbols across {num_slaves} slaves...")
    distribution = distribute_symbols_to_slaves(symbols, num_slaves)
    
    # Show distribution summary
    print("\n📋 Distribution Summary:")
    print("=" * 50)
    for slave_id, data in distribution.items():
        print(f"   {slave_id}: {data['count']} symbols")
        print(f"      Sample: {', '.join(data['symbols'][:5])}")
        if len(data['symbols']) > 5:
            print(f"      ... and {len(data['symbols']) - 5} more")
        print("")
    
    # Save to MongoDB
    print("💾 Saving distribution to MongoDB...")
    if save_distribution_to_mongodb(distribution):
        print("✅ Distribution saved successfully")
    else:
        print("❌ Failed to save distribution")
        return
    
    # Generate slave configs
    print("📝 Generating slave configurations...")
    configs = generate_slave_configs(distribution, master_ip)
    
    # Save configs to files
    os.makedirs('/tmp/slave_configs', exist_ok=True)
    for slave_id, config in configs.items():
        config_file = f'/tmp/slave_configs/{slave_id}.env'
        with open(config_file, 'w') as f:
            for key, value in config.items():
                f.write(f'export {key}="{value}"\n')
        print(f"   {config_file} created")
    
    print("\n🎉 SYMBOL DISTRIBUTION COMPLETE!")
    print("=" * 50)
    print(f"✅ {len(symbols)} symbols distributed across {num_slaves} slaves")
    print("✅ MongoDB updated with distribution")
    print("✅ Slave configuration files generated")
    print("")
    print("Next steps:")
    print("1. Deploy slave applications")
    print("2. Start data collection")

if __name__ == '__main__':
    main()
EOF

# Upload symbol distributor
scp_file "temp_full_symbol_distributor.py" "/home/$USERNAME/autotrader/master/symbol_distributor/full_symbol_distributor.py"
ssh_exec "chmod +x /home/$USERNAME/autotrader/master/symbol_distributor/full_symbol_distributor.py"

# Create Master API server
cat > temp_master_api.py << 'EOF'
#!/usr/bin/env python3
"""
Master VM API Server
Provides REST API for slave coordination and monitoring
"""

import os
from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
from pymongo import MongoClient
from datetime import datetime, timedelta
import json

app = Flask(__name__)
CORS(app)

# MongoDB connection
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://trader:TradingData2025!@10.0.1.100:27017/trading_data')
client = MongoClient(MONGO_URI)
db = client['trading_data']

@app.route('/')
def dashboard():
    """Master dashboard"""
    html = """
    <!DOCTYPE html>
    <html>
    <head><title>AutoTrader Master Dashboard</title></head>
    <body style="font-family: Arial, sans-serif; margin: 40px;">
        <h1>🚀 AutoTrader Master Dashboard</h1>
        <h2>📊 System Status</h2>
        <div id="status"></div>
        
        <h2>⚙️ Slave VMs</h2>
        <div id="slaves"></div>
        
        <h2>📈 Recent Data</h2>
        <div id="data"></div>
        
        <script>
            function loadStatus() {
                fetch('/api/status')
                    .then(r => r.json())
                    .then(data => {
                        document.getElementById('status').innerHTML = 
                            '<p>Active Slaves: ' + data.active_slaves + '</p>' +
                            '<p>Total Symbols: ' + data.total_symbols + '</p>' +
                            '<p>Data Collections Today: ' + data.collections_today + '</p>';
                    });
                    
                fetch('/api/slaves')
                    .then(r => r.json())
                    .then(data => {
                        let html = '<table border="1" style="border-collapse: collapse;"><tr><th>Slave ID</th><th>Symbols</th><th>Last Seen</th><th>Status</th></tr>';
                        data.slaves.forEach(slave => {
                            html += '<tr><td>' + slave.slave_id + '</td><td>' + slave.symbol_count + '</td><td>' + slave.last_seen + '</td><td>' + slave.status + '</td></tr>';
                        });
                        html += '</table>';
                        document.getElementById('slaves').innerHTML = html;
                    });
            }
            
            loadStatus();
            setInterval(loadStatus, 10000);  // Refresh every 10 seconds
        </script>
    </body>
    </html>
    """
    return html

@app.route('/api/status')
def api_status():
    """System status API"""
    try:
        # Count active slaves
        active_slaves = db.symbol_distribution.count_documents({'status': 'active'})
        
        # Count total symbols
        total_symbols = db.symbol_distribution.aggregate([
            {'$group': {'_id': None, 'total': {'$sum': '$symbol_count'}}}
        ])
        total_symbols = list(total_symbols)
        total_symbols = total_symbols[0]['total'] if total_symbols else 0
        
        # Count today's collections
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        collections_today = db.market_data.count_documents({'timestamp': {'$gte': today}})
        
        return jsonify({
            'active_slaves': active_slaves,
            'total_symbols': total_symbols,
            'collections_today': collections_today,
            'timestamp': datetime.utcnow().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/slaves')
def api_slaves():
    """Slave status API"""
    try:
        slaves = list(db.symbol_distribution.find({}, {'_id': 0}))
        
        for slave in slaves:
            # Check last activity
            last_data = db.market_data.find_one(
                {'slave_id': slave['slave_id']}, 
                sort=[('timestamp', -1)]
            )
            if last_data:
                slave['last_seen'] = last_data['timestamp'].isoformat()
                # Check if active in last 5 minutes
                if datetime.utcnow() - last_data['timestamp'] < timedelta(minutes=5):
                    slave['status'] = 'active'
                else:
                    slave['status'] = 'inactive'
            else:
                slave['last_seen'] = 'Never'
                slave['status'] = 'inactive'
        
        return jsonify({'slaves': slaves})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/data/<slave_id>')
def api_slave_data(slave_id):
    """Get recent data from specific slave"""
    try:
        recent_data = list(db.market_data.find(
            {'slave_id': slave_id},
            sort=[('timestamp', -1)],
            limit=10
        ))
        
        # Convert ObjectId to string
        for item in recent_data:
            item['_id'] = str(item['_id'])
            item['timestamp'] = item['timestamp'].isoformat()
        
        return jsonify({'data': recent_data})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/health')
def health():
    """Health check endpoint"""
    try:
        # Test MongoDB connection
        db.admin.command('ping')
        return jsonify({'status': 'healthy', 'timestamp': datetime.utcnow().isoformat()})
    except:
        return jsonify({'status': 'unhealthy', 'timestamp': datetime.utcnow().isoformat()}), 500

if __name__ == '__main__':
    print("🚀 Starting Master API Server...")
    print(f"📊 Dashboard: http://0.0.0.0:8080")
    print(f"🔍 Health: http://0.0.0.0:8080/health")
    app.run(host='0.0.0.0', port=8080, debug=False)
EOF

# Upload Master API
scp_file "temp_master_api.py" "/home/$USERNAME/autotrader/master/api/master_api.py"
ssh_exec "chmod +x /home/$USERNAME/autotrader/master/api/master_api.py"

echo "✅ Master application files deployed"
echo ""

echo -e "${BLUE}🔧 Step 6: Configure Environment${NC}"
echo "--------------------------------"

# Create master environment file
ssh_exec "cat > /home/$USERNAME/autotrader/master.env << EOF
export MONGO_URI=\"mongodb://$MONGO_USERNAME:$MONGO_PASSWORD@10.0.1.100:27017/$MONGO_DB_NAME\"
export MONGO_DB_NAME=\"$MONGO_DB_NAME\"
export MONGO_USERNAME=\"$MONGO_USERNAME\"
export MONGO_PASSWORD=\"$MONGO_PASSWORD\"
export MASTER_VM_IP=\"10.0.1.100\"
export MASTER_PORT=\"$MASTER_PORT\"
export NUM_SLAVES=\"$NUM_SLAVES\"
EOF"

echo "✅ Environment configured"
echo ""

echo -e "${BLUE}🚀 Step 7: Run Symbol Distribution${NC}"
echo "---------------------------------"

# Run symbol distributor
ssh_exec "cd /home/$USERNAME/autotrader && source master.env && python3 master/symbol_distributor/full_symbol_distributor.py"

echo "✅ Symbol distribution complete"
echo ""

echo -e "${BLUE}🎛️ Step 8: Create Service Files${NC}"
echo "------------------------------"

# Create systemd service for Master API
ssh_exec "sudo tee /etc/systemd/system/autotrader-master.service > /dev/null << EOF
[Unit]
Description=AutoTrader Master API
After=network.target mongod.service

[Service]
Type=simple
User=$USERNAME
WorkingDirectory=/home/$USERNAME/autotrader
EnvironmentFile=/home/$USERNAME/autotrader/master.env
ExecStart=/usr/bin/python3 /home/$USERNAME/autotrader/master/api/master_api.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF"

# Enable and start the service
ssh_exec "sudo systemctl daemon-reload"
ssh_exec "sudo systemctl enable autotrader-master.service"
ssh_exec "sudo systemctl start autotrader-master.service"

echo "✅ Master API service created and started"
echo ""

# Clean up temp files
rm -f temp_*.py

echo -e "${GREEN}🎉 MASTER VM DEPLOYMENT COMPLETE!${NC}"
echo "=================================="
echo ""
echo -e "${BLUE}📊 Master VM Status:${NC}"
echo "   🌐 Dashboard: http://$MASTER_IP:8080"
echo "   🔍 Health: http://$MASTER_IP:8080/health"
echo "   🍃 MongoDB: Running on port 27017"
echo "   ⚙️ Master API: Running on port 8080"
echo ""
echo -e "${BLUE}🔧 Service Commands:${NC}"
echo "   sudo systemctl status autotrader-master"
echo "   sudo systemctl restart autotrader-master"
echo "   sudo systemctl logs -f autotrader-master"
echo ""
echo -e "${GREEN}✅ Master VM is ready to coordinate slave VMs!${NC}"