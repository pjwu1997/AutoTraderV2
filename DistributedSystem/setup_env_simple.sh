#!/bin/bash
# 🚀 AutoTrader Azure Deployment - Simple Configuration
# Edit the values below and run this script

set -e

echo "🚀 AUTOTRADER AZURE DEPLOYMENT - SIMPLE SETUP"
echo "=============================================="
echo ""

# ========================================
# 🔧 EDIT THESE VALUES BELOW
# ========================================

# Infrastructure Configuration
export NUM_SLAVES=5                          # Number of slave VMs (3-10 recommended)
export RESOURCE_GROUP="AutoTrader-RG"       # Azure resource group name
export AZURE_REGION="East Asia"             # Azure region
export MASTER_VM_SIZE="Standard_B2s"        # Master VM size
export SLAVE_VM_SIZE="Standard_B1s"         # Slave VM size

# VM Authentication - ⚠️ CHANGE THESE PASSWORDS!
export VM_ADMIN_USERNAME="azureuser"        # VM admin username
export VM_ADMIN_PASSWORD="6s0NeCqpAhDG"  # ⚠️ CHANGE THIS PASSWORD!

# Network Configuration
export VNET_NAME="AutoTrader-VNet"          # Virtual network name
export SUBNET_NAME="AutoTrader-Subnet"      # Subnet name
export NSG_NAME="AutoTrader-NSG"            # Network security group name
export MASTER_VM_IP="10.0.1.100"           # Master VM internal IP

# Application Configuration
export MONGO_USERNAME="trader"              # MongoDB username
export MONGO_PASSWORD="TradingData2025!"    # ⚠️ CHANGE THIS PASSWORD!
export MONGO_DB_NAME="trading_data"         # MongoDB database name
export MASTER_PORT="8080"                   # Master API port

# Trading Configuration
export TIMEFRAME="1m"                       # Data collection timeframe
export FETCH_INTERVAL="60"                  # Collection interval in seconds
export BATCH_SIZE="100"                     # Batch size per slave

# ========================================
# 🧮 AUTOMATIC CALCULATIONS (DO NOT EDIT)
# ========================================

# Calculate derived values
export TOTAL_VMS=$((NUM_SLAVES + 1))
export TOTAL_IPS=$TOTAL_VMS
export SUBNET_CIDR="10.0.1.0/24"
export VNET_CIDR="10.0.0.0/16"

# Generate MongoDB URI
export MONGO_URI="mongodb://${MONGO_USERNAME}:${MONGO_PASSWORD}@${MASTER_VM_IP}:27017/${MONGO_DB_NAME}"

# Cost calculation (TWD)
export MASTER_COST=1200
export SLAVE_UNIT_COST=500
export IP_UNIT_COST=30
export STORAGE_COST=1200
export TOTAL_MONTHLY_COST=$((MASTER_COST + (NUM_SLAVES * SLAVE_UNIT_COST) + (TOTAL_VMS * IP_UNIT_COST) + STORAGE_COST))

# ========================================
# 📋 CONFIGURATION SUMMARY
# ========================================

echo "📋 DEPLOYMENT CONFIGURATION:"
echo "============================="
echo ""
echo "🏗️  Infrastructure:"
echo "   Resource Group: $RESOURCE_GROUP"
echo "   Region: $AZURE_REGION"
echo "   Total VMs: $TOTAL_VMS (1 Master + $NUM_SLAVES Slaves)"
echo "   Master Size: $MASTER_VM_SIZE"
echo "   Slave Size: $SLAVE_VM_SIZE"
echo ""
echo "🌐 Network:"
echo "   VNet: $VNET_NAME ($VNET_CIDR)"
echo "   Subnet: $SUBNET_NAME ($SUBNET_CIDR)"
echo "   Master IP: $MASTER_VM_IP"
echo "   NSG: $NSG_NAME"
echo ""
echo "⚙️  Application:"
echo "   MongoDB Database: $MONGO_DB_NAME"
echo "   MongoDB Username: $MONGO_USERNAME"
echo "   Master Port: $MASTER_PORT"
echo "   Timeframe: $TIMEFRAME"
echo "   Batch Size: $BATCH_SIZE per slave"
echo ""
echo "💰 Cost Estimate:"
echo "   Monthly Cost: $TOTAL_MONTHLY_COST TWD"
echo "   Master VM: $MASTER_COST TWD"
echo "   Slave VMs: $((NUM_SLAVES * SLAVE_UNIT_COST)) TWD ($NUM_SLAVES × $SLAVE_UNIT_COST)"
echo "   Public IPs: $((TOTAL_VMS * IP_UNIT_COST)) TWD ($TOTAL_VMS × $IP_UNIT_COST)"
echo "   Storage: $STORAGE_COST TWD"
echo ""

# ========================================
# 💾 SAVE CONFIGURATION
# ========================================

echo "💾 Saving configuration..."

ENV_FILE="deployment.env"
cat > "$ENV_FILE" << EOF
# AutoTrader Azure Deployment Configuration
# Generated on: $(date)

# Infrastructure
export NUM_SLAVES=$NUM_SLAVES
export RESOURCE_GROUP="$RESOURCE_GROUP"
export AZURE_REGION="$AZURE_REGION"
export MASTER_VM_SIZE="$MASTER_VM_SIZE"
export SLAVE_VM_SIZE="$SLAVE_VM_SIZE"

# VM Authentication
export VM_ADMIN_USERNAME="$VM_ADMIN_USERNAME"
export VM_ADMIN_PASSWORD="$VM_ADMIN_PASSWORD"

# Network
export VNET_NAME="$VNET_NAME"
export SUBNET_NAME="$SUBNET_NAME"
export NSG_NAME="$NSG_NAME"
export MASTER_VM_IP="$MASTER_VM_IP"
export VNET_CIDR="$VNET_CIDR"
export SUBNET_CIDR="$SUBNET_CIDR"

# Application
export MONGO_USERNAME="$MONGO_USERNAME"
export MONGO_PASSWORD="$MONGO_PASSWORD"
export MONGO_DB_NAME="$MONGO_DB_NAME"
export MONGO_URI="$MONGO_URI"
export MASTER_PORT="$MASTER_PORT"

# Trading Configuration
export TIMEFRAME="$TIMEFRAME"
export FETCH_INTERVAL="$FETCH_INTERVAL"
export BATCH_SIZE="$BATCH_SIZE"

# Calculated Values
export TOTAL_VMS=$TOTAL_VMS
export TOTAL_IPS=$TOTAL_IPS
export TOTAL_MONTHLY_COST=$TOTAL_MONTHLY_COST
EOF

echo "✅ Configuration saved to: $ENV_FILE"
echo ""

# ========================================
# ⚠️  SECURITY VALIDATION
# ========================================

echo "🔒 Security Validation:"
echo "======================="

if [ "$VM_ADMIN_PASSWORD" = "AutoTrader2025!" ]; then
    echo "⚠️  WARNING: Using default VM admin password!"
    echo "   Please edit the script and change VM_ADMIN_PASSWORD"
fi

if [ "$MONGO_PASSWORD" = "TradingData2025!" ]; then
    echo "⚠️  WARNING: Using default MongoDB password!"
    echo "   Please edit the script and change MONGO_PASSWORD"
fi

echo ""

# ========================================
# 🎯 NEXT STEPS
# ========================================

echo "🎯 NEXT STEPS:"
echo "=============="
echo ""
echo "1. Review and edit passwords in this script if needed:"
echo "   nano $0"
echo ""
echo "2. Source the environment file:"
echo "   source $ENV_FILE"
echo ""
echo "3. Login to Azure:"
echo "   az login"
echo ""
echo "4. Deploy infrastructure:"
echo "   ./deploy_azure_infrastructure.sh"
echo ""

# ========================================
# 🔄 AUTO-LOAD ENVIRONMENT
# ========================================

echo "🔄 Loading environment variables..."
source "$ENV_FILE"
echo "✅ Environment variables loaded into current session!"
echo ""

echo "🎉 SETUP COMPLETE!"
echo ""
echo "Environment is ready. You can now run:"
echo "   az login"
echo "   ./deploy_azure_infrastructure.sh"