#!/bin/bash
# Test SSH connectivity to all VMs with automatic fingerprint acceptance

set -e

echo "🔐 TESTING SSH CONNECTIVITY TO ALL VMs"
echo "======================================="
echo ""

# Load deployment info
source deployment.env

# VM credentials
USERNAME="$VM_ADMIN_USERNAME"
PASSWORD="$VM_ADMIN_PASSWORD"

echo "📋 Configuration:"
echo "   Username: $USERNAME"
echo "   Password: [CONFIGURED]"
echo ""

# SSH options to auto-accept fingerprints and handle password
SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o BatchMode=no"

# Function to test SSH connection
test_ssh() {
    local vm_name="$1"
    local ip="$2"
    
    echo "Testing $vm_name ($ip)..."
    
    # Use expect to handle password prompt
    expect << EOF
set timeout 15
spawn ssh $SSH_OPTS $USERNAME@$ip "echo '✅ SSH successful to $vm_name'; hostname; uptime"
expect {
    "password:" {
        send "$PASSWORD\r"
        expect {
            "✅ SSH successful" {
                puts "✅ $vm_name: SSH connection successful"
                exp_continue
            }
            timeout {
                puts "❌ $vm_name: SSH timeout after password"
                exit 1
            }
        }
    }
    "Connection refused" {
        puts "⚠️  $vm_name: SSH service not ready yet"
        exit 1
    }
    timeout {
        puts "⚠️  $vm_name: Connection timeout"
        exit 1
    }
}
expect eof
EOF

    if [ $? -eq 0 ]; then
        echo "✅ $vm_name: Connection successful"
    else
        echo "⚠️  $vm_name: Connection failed (VM might still be starting)"
    fi
    echo ""
}

# Check if expect is available
if ! command -v expect &> /dev/null; then
    echo "❌ 'expect' command not found. Installing..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        if command -v brew &> /dev/null; then
            brew install expect
        else
            echo "Please install expect: brew install expect"
            exit 1
        fi
    else
        # Linux
        sudo apt-get update && sudo apt-get install -y expect
    fi
fi

echo "🔍 Testing connectivity to all VMs..."
echo ""

# Test Master VM
test_ssh "Master VM" "20.255.100.73"

# Test Slave VMs
test_ssh "Slave VM 1" "52.175.36.139"
test_ssh "Slave VM 2" "20.2.4.16"
test_ssh "Slave VM 3" "20.2.117.128"
test_ssh "Slave VM 4" "20.2.4.203"
test_ssh "Slave VM 5" "20.2.12.241"

echo "🎯 SSH CONNECTIVITY TEST COMPLETE"
echo ""
echo "💡 If any VMs show 'not ready', wait 2-3 minutes and run this script again."
echo "   VMs need time to fully boot and start SSH service."