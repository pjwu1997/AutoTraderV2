#!/bin/bash
# 🚀 AutoTrader Azure Deployment Environment Setup
# This script sets up all required environment variables for Azure deployment

set -e

echo "🚀 AUTOTRADER AZURE DEPLOYMENT SETUP"
echo "===================================="
echo ""

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to prompt for input with default value
prompt_with_default() {
    local prompt="$1"
    local default="$2"
    local var_name="$3"
    local is_password="$4"
    
    if [ "$is_password" = "true" ]; then
        echo -n -e "${BLUE}$prompt${NC} [default: $default]: "
        read -s user_input
        echo ""
    else
        echo -n -e "${BLUE}$prompt${NC} [default: $default]: "
        read user_input
    fi
    
    if [ -z "$user_input" ]; then
        export $var_name="$default"
    else
        export $var_name="$user_input"
    fi
}

echo -e "${YELLOW}📋 This script will set up your Azure deployment configuration.${NC}"
echo -e "${YELLOW}   You can press ENTER to use default values or enter your own.${NC}"
echo ""

# ========================================
# AZURE INFRASTRUCTURE CONFIGURATION
# ========================================
echo -e "${GREEN}🏗️  AZURE INFRASTRUCTURE CONFIGURATION${NC}"
echo "----------------------------------------"

prompt_with_default "Number of Slave VMs (3-10 recommended)" "5" "NUM_SLAVES"
prompt_with_default "Azure Resource Group name" "AutoTrader-RG" "RESOURCE_GROUP"
prompt_with_default "Azure Region" "East Asia" "AZURE_REGION"
prompt_with_default "Master VM Size" "Standard_B2s" "MASTER_VM_SIZE"
prompt_with_default "Slave VM Size" "Standard_B1s" "SLAVE_VM_SIZE"

echo ""

# ========================================
# VM AUTHENTICATION
# ========================================
echo -e "${GREEN}🔐 VM AUTHENTICATION SETUP${NC}"
echo "----------------------------"

prompt_with_default "VM Admin Username" "azureuser" "VM_ADMIN_USERNAME"
prompt_with_default "VM Admin Password (SECURE!)" "AutoTrader2025!" "VM_ADMIN_PASSWORD" "true"

echo ""

# ========================================
# NETWORK CONFIGURATION
# ========================================
echo -e "${GREEN}🌐 NETWORK CONFIGURATION${NC}"
echo "-------------------------"

prompt_with_default "Virtual Network Name" "AutoTrader-VNet" "VNET_NAME"
prompt_with_default "Subnet Name" "AutoTrader-Subnet" "SUBNET_NAME"
prompt_with_default "Network Security Group Name" "AutoTrader-NSG" "NSG_NAME"
prompt_with_default "Master VM Internal IP" "10.0.1.100" "MASTER_VM_IP"

echo ""

# ========================================
# APPLICATION CONFIGURATION
# ========================================
echo -e "${GREEN}⚙️  APPLICATION CONFIGURATION${NC}"
echo "-------------------------------"

prompt_with_default "MongoDB Username" "trader" "MONGO_USERNAME"
prompt_with_default "MongoDB Password" "TradingData2025!" "MONGO_PASSWORD" "true"
prompt_with_default "MongoDB Database Name" "trading_data" "MONGO_DB_NAME"
prompt_with_default "Master Port" "8080" "MASTER_PORT"

echo ""

# ========================================
# TRADING CONFIGURATION
# ========================================
echo -e "${GREEN}📊 TRADING CONFIGURATION${NC}"
echo "-------------------------"

prompt_with_default "Data Collection Timeframe" "1m" "TIMEFRAME"
prompt_with_default "Collection Interval (seconds)" "60" "FETCH_INTERVAL"
prompt_with_default "Batch Size per Slave" "100" "BATCH_SIZE"

echo ""

# ========================================
# CALCULATED VALUES
# ========================================
echo -e "${GREEN}🧮 CALCULATING DEPLOYMENT VALUES...${NC}"

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

echo ""

# ========================================
# CONFIGURATION SUMMARY
# ========================================
echo -e "${YELLOW}📋 DEPLOYMENT CONFIGURATION SUMMARY${NC}"
echo "====================================="
echo ""
echo -e "${BLUE}🏗️  Infrastructure:${NC}"
echo "   Resource Group: $RESOURCE_GROUP"
echo "   Region: $AZURE_REGION"
echo "   Total VMs: $TOTAL_VMS (1 Master + $NUM_SLAVES Slaves)"
echo "   Master Size: $MASTER_VM_SIZE"
echo "   Slave Size: $SLAVE_VM_SIZE"
echo ""
echo -e "${BLUE}🌐 Network:${NC}"
echo "   VNet: $VNET_NAME ($VNET_CIDR)"
echo "   Subnet: $SUBNET_NAME ($SUBNET_CIDR)"
echo "   Master IP: $MASTER_VM_IP"
echo "   NSG: $NSG_NAME"
echo ""
echo -e "${BLUE}⚙️  Application:${NC}"
echo "   MongoDB: $MONGO_DB_NAME"
echo "   Master Port: $MASTER_PORT"
echo "   Timeframe: $TIMEFRAME"
echo "   Batch Size: $BATCH_SIZE per slave"
echo ""
echo -e "${BLUE}💰 Cost Estimate:${NC}"
echo "   Monthly Cost: $TOTAL_MONTHLY_COST TWD"
echo "   Master VM: $MASTER_COST TWD"
echo "   Slave VMs: $((NUM_SLAVES * SLAVE_UNIT_COST)) TWD"
echo "   Public IPs: $((TOTAL_VMS * IP_UNIT_COST)) TWD"
echo "   Storage: $STORAGE_COST TWD"
echo ""

# ========================================
# SAVE CONFIGURATION
# ========================================
echo -e "${GREEN}💾 SAVING CONFIGURATION...${NC}"

# Create environment file
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
# NEXT STEPS
# ========================================
echo -e "${YELLOW}🎯 NEXT STEPS${NC}"
echo "============"
echo ""
echo "1. Source the environment file:"
echo -e "   ${GREEN}source $ENV_FILE${NC}"
echo ""
echo "2. Login to Azure:"
echo -e "   ${GREEN}az login${NC}"
echo ""
echo "3. Run the deployment script:"
echo -e "   ${GREEN}./deploy_azure_infrastructure.sh${NC}"
echo ""
echo -e "${RED}⚠️  IMPORTANT SECURITY NOTES:${NC}"
echo "   • Keep deployment.env file secure (contains passwords)"
echo "   • Consider adding deployment.env to .gitignore"
echo "   • Change default passwords in production"
echo ""

# ========================================
# LOAD ENVIRONMENT
# ========================================
echo -e "${BLUE}🔄 Loading environment variables into current session...${NC}"
source "$ENV_FILE"
echo "✅ Environment variables loaded!"
echo ""
echo -e "${GREEN}🎉 SETUP COMPLETE!${NC}"
echo "You can now proceed with Azure deployment."
echo ""
echo "To reload these variables later, run:"
echo -e "${GREEN}source $ENV_FILE${NC}"