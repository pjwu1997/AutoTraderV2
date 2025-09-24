#!/bin/bash
# 🚀 AutoTrader Azure Infrastructure Deployment Script
# Creates complete Azure infrastructure for Master and Slave VMs

set -e

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${GREEN}🚀 AUTOTRADER AZURE INFRASTRUCTURE DEPLOYMENT${NC}"
echo "=============================================="
echo ""

# Check if environment is loaded
if [ -z "$RESOURCE_GROUP" ]; then
    echo -e "${RED}❌ Environment variables not loaded!${NC}"
    echo "Please run: source deployment.env"
    echo "Or run: ./setup_deployment_env.sh first"
    exit 1
fi

# Display configuration
echo -e "${BLUE}📋 Deployment Configuration:${NC}"
echo "   Resource Group: $RESOURCE_GROUP"
echo "   Region: $AZURE_REGION"
echo "   VMs: 1 Master + $NUM_SLAVES Slaves"
echo "   Estimated Cost: $TOTAL_MONTHLY_COST TWD/month"
echo ""

# Confirmation prompt
echo -e "${YELLOW}⚠️  This will create Azure resources that incur costs!${NC}"
echo -n "Do you want to proceed? (y/N): "
read confirmation
if [[ ! "$confirmation" =~ ^[Yy]$ ]]; then
    echo "Deployment cancelled."
    exit 0
fi

echo ""
echo -e "${GREEN}🏗️  Starting Azure infrastructure deployment...${NC}"
echo ""

# ========================================
# STEP 1: CREATE RESOURCE GROUP
# ========================================
echo -e "${BLUE}📦 Step 1: Creating Resource Group${NC}"
echo "-----------------------------------"

if az group show --name "$RESOURCE_GROUP" &>/dev/null; then
    echo "✅ Resource group '$RESOURCE_GROUP' already exists"
else
    echo "Creating resource group '$RESOURCE_GROUP' in '$AZURE_REGION'..."
    az group create \
        --name "$RESOURCE_GROUP" \
        --location "$AZURE_REGION"
    echo "✅ Resource group created"
fi
echo ""

# ========================================
# STEP 2: CREATE VIRTUAL NETWORK
# ========================================
echo -e "${BLUE}🌐 Step 2: Creating Virtual Network${NC}"
echo "----------------------------------"

echo "Creating virtual network '$VNET_NAME'..."
az network vnet create \
    --resource-group "$RESOURCE_GROUP" \
    --name "$VNET_NAME" \
    --address-prefix "$VNET_CIDR" \
    --subnet-name "$SUBNET_NAME" \
    --subnet-prefix "$SUBNET_CIDR" \
    --location "$AZURE_REGION"

echo "✅ Virtual network created"
echo ""

# ========================================
# STEP 3: CREATE NETWORK SECURITY GROUP
# ========================================
echo -e "${BLUE}🔒 Step 3: Creating Network Security Group${NC}"
echo "----------------------------------------"

echo "Creating network security group '$NSG_NAME'..."
az network nsg create \
    --resource-group "$RESOURCE_GROUP" \
    --name "$NSG_NAME" \
    --location "$AZURE_REGION"

# Add security rules
echo "Adding security rules..."

# SSH access
az network nsg rule create \
    --resource-group "$RESOURCE_GROUP" \
    --nsg-name "$NSG_NAME" \
    --name "AllowSSH" \
    --protocol Tcp \
    --priority 1000 \
    --destination-port-range 22 \
    --access Allow \
    --description "Allow SSH"

# Master API port
az network nsg rule create \
    --resource-group "$RESOURCE_GROUP" \
    --nsg-name "$NSG_NAME" \
    --name "AllowMasterAPI" \
    --protocol Tcp \
    --priority 1010 \
    --destination-port-range "$MASTER_PORT" \
    --access Allow \
    --description "Allow Master API"

# MongoDB port (internal only)
az network nsg rule create \
    --resource-group "$RESOURCE_GROUP" \
    --nsg-name "$NSG_NAME" \
    --name "AllowMongoInternal" \
    --protocol Tcp \
    --priority 1020 \
    --destination-port-range 27017 \
    --source-address-prefix "$SUBNET_CIDR" \
    --access Allow \
    --description "Allow MongoDB internal"

# HTTP/HTTPS for web access
az network nsg rule create \
    --resource-group "$RESOURCE_GROUP" \
    --nsg-name "$NSG_NAME" \
    --name "AllowHTTP" \
    --protocol Tcp \
    --priority 1030 \
    --destination-port-range 80 \
    --access Allow \
    --description "Allow HTTP"

az network nsg rule create \
    --resource-group "$RESOURCE_GROUP" \
    --nsg-name "$NSG_NAME" \
    --name "AllowHTTPS" \
    --protocol Tcp \
    --priority 1040 \
    --destination-port-range 443 \
    --access Allow \
    --description "Allow HTTPS"

echo "✅ Network security group created with rules"
echo ""

# ========================================
# STEP 4: CREATE MASTER VM
# ========================================
echo -e "${BLUE}🖥️  Step 4: Creating Master VM${NC}"
echo "-----------------------------"

MASTER_VM_NAME="AutoTrader-Master"
echo "Creating Master VM '$MASTER_VM_NAME'..."

az vm create \
    --resource-group "$RESOURCE_GROUP" \
    --name "$MASTER_VM_NAME" \
    --image "Ubuntu2204" \
    --size "$MASTER_VM_SIZE" \
    --admin-username "$VM_ADMIN_USERNAME" \
    --admin-password "$VM_ADMIN_PASSWORD" \
    --authentication-type password \
    --vnet-name "$VNET_NAME" \
    --subnet "$SUBNET_NAME" \
    --private-ip-address "$MASTER_VM_IP" \
    --nsg "$NSG_NAME" \
    --public-ip-sku Standard \
    --location "$AZURE_REGION" \
    --storage-sku Premium_LRS \
    --os-disk-size-gb 128

echo "✅ Master VM created"

# Get Master VM public IP
MASTER_PUBLIC_IP=$(az vm show -d -g "$RESOURCE_GROUP" -n "$MASTER_VM_NAME" --query publicIps -o tsv)
echo "📍 Master VM Public IP: $MASTER_PUBLIC_IP"
echo ""

# ========================================
# STEP 5: CREATE SLAVE VMs
# ========================================
echo -e "${BLUE}⚙️  Step 5: Creating Slave VMs${NC}"
echo "-----------------------------"

SLAVE_PUBLIC_IPS=()

for i in $(seq 1 $NUM_SLAVES); do
    SLAVE_VM_NAME="AutoTrader-Slave-$i"
    SLAVE_PRIVATE_IP="10.0.1.$((100 + i))"
    
    echo "Creating Slave VM $i/$NUM_SLAVES: '$SLAVE_VM_NAME'..."
    
    az vm create \
        --resource-group "$RESOURCE_GROUP" \
        --name "$SLAVE_VM_NAME" \
        --image "Ubuntu2204" \
        --size "$SLAVE_VM_SIZE" \
        --admin-username "$VM_ADMIN_USERNAME" \
        --admin-password "$VM_ADMIN_PASSWORD" \
        --authentication-type password \
        --vnet-name "$VNET_NAME" \
        --subnet "$SUBNET_NAME" \
        --private-ip-address "$SLAVE_PRIVATE_IP" \
        --nsg "$NSG_NAME" \
        --public-ip-sku Standard \
        --location "$AZURE_REGION" \
        --storage-sku Standard_LRS \
        --os-disk-size-gb 64 \
        --no-wait
    
    echo "✅ Slave VM $i queued for creation"
done

echo ""
echo "⏳ Waiting for all Slave VMs to be created..."
echo ""

# Wait for all VMs to be ready and collect public IPs
for i in $(seq 1 $NUM_SLAVES); do
    SLAVE_VM_NAME="AutoTrader-Slave-$i"
    echo "Waiting for $SLAVE_VM_NAME to be ready..."
    
    az vm wait --resource-group "$RESOURCE_GROUP" --name "$SLAVE_VM_NAME" --created
    
    SLAVE_PUBLIC_IP=$(az vm show -d -g "$RESOURCE_GROUP" -n "$SLAVE_VM_NAME" --query publicIps -o tsv)
    SLAVE_PUBLIC_IPS+=("$SLAVE_PUBLIC_IP")
    
    echo "✅ $SLAVE_VM_NAME ready - Public IP: $SLAVE_PUBLIC_IP"
done

echo ""

# ========================================
# STEP 6: SAVE DEPLOYMENT INFO
# ========================================
echo -e "${BLUE}💾 Step 6: Saving Deployment Information${NC}"
echo "---------------------------------------"

DEPLOYMENT_INFO_FILE="deployment_info.json"
cat > "$DEPLOYMENT_INFO_FILE" << EOF
{
    "deployment_timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
    "resource_group": "$RESOURCE_GROUP",
    "region": "$AZURE_REGION",
    "master_vm": {
        "name": "$MASTER_VM_NAME",
        "size": "$MASTER_VM_SIZE",
        "private_ip": "$MASTER_VM_IP",
        "public_ip": "$MASTER_PUBLIC_IP"
    },
    "slave_vms": [
EOF

for i in $(seq 1 $NUM_SLAVES); do
    SLAVE_VM_NAME="AutoTrader-Slave-$i"
    SLAVE_PRIVATE_IP="10.0.1.$((100 + i))"
    SLAVE_PUBLIC_IP="${SLAVE_PUBLIC_IPS[$((i-1))]}"
    
    cat >> "$DEPLOYMENT_INFO_FILE" << EOF
        {
            "name": "$SLAVE_VM_NAME",
            "size": "$SLAVE_VM_SIZE",
            "private_ip": "$SLAVE_PRIVATE_IP",
            "public_ip": "$SLAVE_PUBLIC_IP"
        }$([ $i -lt $NUM_SLAVES ] && echo "," || echo "")
EOF
done

cat >> "$DEPLOYMENT_INFO_FILE" << EOF
    ],
    "network": {
        "vnet_name": "$VNET_NAME",
        "subnet_name": "$SUBNET_NAME",
        "nsg_name": "$NSG_NAME"
    },
    "estimated_monthly_cost_twd": $TOTAL_MONTHLY_COST
}
EOF

echo "✅ Deployment info saved to: $DEPLOYMENT_INFO_FILE"
echo ""

# ========================================
# DEPLOYMENT SUMMARY
# ========================================
echo -e "${GREEN}🎉 AZURE INFRASTRUCTURE DEPLOYMENT COMPLETE!${NC}"
echo "============================================="
echo ""
echo -e "${BLUE}📋 Deployment Summary:${NC}"
echo ""
echo -e "${YELLOW}🖥️  Master VM:${NC}"
echo "   Name: $MASTER_VM_NAME"
echo "   Public IP: $MASTER_PUBLIC_IP"
echo "   Private IP: $MASTER_VM_IP"
echo "   Size: $MASTER_VM_SIZE"
echo ""
echo -e "${YELLOW}⚙️  Slave VMs:${NC}"
for i in $(seq 1 $NUM_SLAVES); do
    SLAVE_VM_NAME="AutoTrader-Slave-$i"
    SLAVE_PRIVATE_IP="10.0.1.$((100 + i))"
    SLAVE_PUBLIC_IP="${SLAVE_PUBLIC_IPS[$((i-1))]}"
    echo "   $SLAVE_VM_NAME: $SLAVE_PUBLIC_IP (private: $SLAVE_PRIVATE_IP)"
done
echo ""
echo -e "${YELLOW}💰 Cost Information:${NC}"
echo "   Estimated Monthly Cost: $TOTAL_MONTHLY_COST TWD"
echo "   Resource Group: $RESOURCE_GROUP"
echo "   Region: $AZURE_REGION"
echo ""

# ========================================
# NEXT STEPS
# ========================================
echo -e "${BLUE}🎯 NEXT STEPS:${NC}"
echo ""
echo "1. Test SSH connectivity:"
echo -e "   ${GREEN}ssh $VM_ADMIN_USERNAME@$MASTER_PUBLIC_IP${NC}"
echo ""
echo "2. Deploy Master VM application:"
echo -e "   ${GREEN}./deploy_master_application.sh $MASTER_PUBLIC_IP${NC}"
echo ""
echo "3. Deploy Slave VM applications:"
for i in $(seq 1 $NUM_SLAVES); do
    SLAVE_PUBLIC_IP="${SLAVE_PUBLIC_IPS[$((i-1))]}"
    echo -e "   ${GREEN}./deploy_slave_application.sh $SLAVE_PUBLIC_IP slave-$i${NC}"
done
echo ""
echo -e "${RED}⚠️  Remember to:${NC}"
echo "   • Keep deployment credentials secure"
echo "   • Monitor Azure costs"
echo "   • Configure SSL/TLS for production"
echo "   • Set up monitoring and backups"
echo ""
echo -e "${GREEN}Infrastructure is ready! 🚀${NC}"