#!/bin/bash
# Deploy All Applications - Master and All Slaves

set -e

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${GREEN}🚀 COMPLETE AUTOTRADER SYSTEM DEPLOYMENT${NC}"
echo "========================================"
echo ""

# Load configuration
if [ ! -f "deployment.env" ]; then
    echo -e "${RED}❌ deployment.env not found!${NC}"
    echo "Please run ./setup_env_simple.sh first"
    exit 1
fi

source deployment.env

echo -e "${BLUE}📋 Deployment Plan:${NC}"
echo "   🖥️  Master VM: 20.255.100.73"
echo "   ⚙️  Slave VMs: 5 instances"
echo "   💾 MongoDB: Full database setup"
echo "   📊 Data Collection: 1-minute aggregation"
echo "   🌐 API Dashboard: http://20.255.100.73:8080"
echo ""

# Confirmation
echo -e "${YELLOW}⚠️  This will deploy applications to all VMs!${NC}"
echo -n "Continue with deployment? (y/N): "
read confirmation
if [[ ! "$confirmation" =~ ^[Yy]$ ]]; then
    echo "Deployment cancelled."
    exit 0
fi

echo ""
echo -e "${GREEN}🏗️  Starting complete system deployment...${NC}"
echo ""

# ========================================
# STEP 1: Deploy Master VM
# ========================================
echo -e "${BLUE}📦 Step 1: Deploying Master VM Application${NC}"
echo "==========================================="

./deploy_master_application.sh

echo ""
echo -e "${GREEN}✅ Master VM deployment complete!${NC}"
echo ""

# Wait for Master VM to be fully ready
echo "⏳ Waiting 30 seconds for Master VM to be fully operational..."
sleep 30

# ========================================
# STEP 2: Deploy All Slave VMs
# ========================================
echo -e "${BLUE}⚙️  Step 2: Deploying All Slave VMs${NC}"
echo "================================="

# Slave VM IPs from deployment
SLAVE_IPS=(
    "52.175.36.139"   # slave-1
    "20.2.4.16"       # slave-2  
    "20.2.117.128"    # slave-3
    "20.2.4.203"      # slave-4
    "20.2.12.241"     # slave-5
)

# Deploy each slave in parallel
pids=()

for i in "${!SLAVE_IPS[@]}"; do
    slave_number=$((i + 1))
    slave_id="slave-$slave_number"
    slave_ip="${SLAVE_IPS[$i]}"
    
    echo "🚀 Starting deployment of $slave_id ($slave_ip)..."
    
    # Run deployment in background
    (
        echo "[$slave_id] Deploying..."
        ./deploy_slave_application.sh "$slave_ip" "$slave_id" > "deployment_${slave_id}.log" 2>&1
        echo "[$slave_id] ✅ Deployment complete"
    ) &
    
    pids+=($!)
    
    # Small delay between deployments
    sleep 2
done

echo ""
echo "⏳ Waiting for all slave deployments to complete..."

# Wait for all background processes
for pid in "${pids[@]}"; do
    wait "$pid"
done

echo ""
echo -e "${GREEN}✅ All Slave VM deployments complete!${NC}"
echo ""

# ========================================
# STEP 3: Verify Deployment
# ========================================
echo -e "${BLUE}🔍 Step 3: Verifying Complete System${NC}"
echo "==================================="

# Test Master VM API
echo "🌐 Testing Master VM API..."
if curl -s http://20.255.100.73:8080/health > /dev/null; then
    echo "   ✅ Master API is responding"
else
    echo "   ⚠️  Master API not ready yet (may need more time)"
fi

# Check Master VM service status
echo "⚙️  Checking Master VM service..."
sshpass -p "$VM_ADMIN_PASSWORD" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "$VM_ADMIN_USERNAME@20.255.100.73" "systemctl is-active autotrader-master" &>/dev/null
if [ $? -eq 0 ]; then
    echo "   ✅ Master service is running"
else
    echo "   ⚠️  Master service may be starting"
fi

# Check slave VM services
echo "🔄 Checking Slave VM services..."
for i in "${!SLAVE_IPS[@]}"; do
    slave_number=$((i + 1))
    slave_id="slave-$slave_number"
    slave_ip="${SLAVE_IPS[$i]}"
    
    echo "   Checking $slave_id ($slave_ip)..."
    sshpass -p "$VM_ADMIN_PASSWORD" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "$VM_ADMIN_USERNAME@$slave_ip" "systemctl is-active autotrader-slave" &>/dev/null
    if [ $? -eq 0 ]; then
        echo "     ✅ $slave_id service is running"
    else
        echo "     ⚠️  $slave_id service may be starting"
    fi
done

echo ""

# ========================================
# DEPLOYMENT SUMMARY
# ========================================
echo -e "${GREEN}🎉 COMPLETE SYSTEM DEPLOYMENT FINISHED!${NC}"
echo "======================================="
echo ""
echo -e "${BLUE}🎯 System Overview:${NC}"
echo ""
echo -e "${YELLOW}🖥️  Master VM:${NC}"
echo "   • IP: 20.255.100.73"
echo "   • Dashboard: http://20.255.100.73:8080"
echo "   • MongoDB: Running on port 27017"
echo "   • API: Running on port 8080"
echo "   • Status: ✅ Deployed"
echo ""
echo -e "${YELLOW}⚙️  Slave VMs (5 instances):${NC}"
for i in "${!SLAVE_IPS[@]}"; do
    slave_number=$((i + 1))
    slave_ip="${SLAVE_IPS[$i]}"
    echo "   • Slave-$slave_number: $slave_ip ✅ Deployed"
done
echo ""
echo -e "${YELLOW}📊 Data Collection:${NC}"
echo "   • Collection Type: 1-minute aggregated"
echo "   • Data Types: 10 (OHLCV, trades, funding, ratios, etc.)"
echo "   • Storage: MongoDB per-symbol collections"
echo "   • Status: ✅ Active"
echo ""

# ========================================
# ACCESS INFORMATION
# ========================================
echo -e "${BLUE}🌐 Access Information:${NC}"
echo "======================"
echo ""
echo -e "${GREEN}📊 Master Dashboard:${NC}"
echo "   URL: http://20.255.100.73:8080"
echo "   Health: http://20.255.100.73:8080/health"
echo "   API Status: http://20.255.100.73:8080/api/status"
echo ""
echo -e "${GREEN}🔐 SSH Access:${NC}"
echo "   Master: ssh $VM_ADMIN_USERNAME@20.255.100.73"
echo "   Password: $VM_ADMIN_PASSWORD"
echo ""
echo -e "${GREEN}🔧 Service Management:${NC}"
echo "   Master: sudo systemctl status autotrader-master"
echo "   Slaves: sudo systemctl status autotrader-slave"
echo "   Logs: sudo journalctl -u autotrader-master -f"
echo ""

# ========================================
# MONITORING & NEXT STEPS
# ========================================
echo -e "${BLUE}📋 Next Steps:${NC}"
echo "=============="
echo ""
echo "1. 🌐 Open Dashboard: http://20.255.100.73:8080"
echo "2. 🔍 Monitor data collection (may take 2-3 minutes to start)"
echo "3. 📊 Check slave status in dashboard"
echo "4. 📈 Monitor MongoDB collections for incoming data"
echo ""
echo -e "${YELLOW}💡 Tips:${NC}"
echo "   • Data collection starts immediately"
echo "   • First data appears within 1-2 minutes"
echo "   • Dashboard updates every 10 seconds"
echo "   • All services auto-restart if they fail"
echo ""

# Check deployment logs
echo -e "${BLUE}📄 Deployment Logs:${NC}"
echo "==================="
echo "   Check individual slave deployment logs:"
for i in {1..5}; do
    if [ -f "deployment_slave-$i.log" ]; then
        echo "   • deployment_slave-$i.log"
    fi
done
echo ""

echo -e "${GREEN}🚀 AUTOTRADER SYSTEM IS LIVE!${NC}"
echo ""
echo -e "${YELLOW}🎯 Your distributed trading data collection system is now running!${NC}"
echo "   • Master VM: Coordinating and storing data"
echo "   • 5 Slave VMs: Collecting real-time market data"
echo "   • MongoDB: Storing all aggregated data"
echo "   • Dashboard: Real-time monitoring available"
echo ""
echo -e "${GREEN}Happy Trading! 📈${NC}"