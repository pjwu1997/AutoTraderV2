#!/bin/bash
# Quick SSH connectivity test

echo "🔐 QUICK SSH TEST"
echo "=================="

# Install sshpass if not available (macOS)
if ! command -v sshpass &> /dev/null; then
    echo "Installing sshpass..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        if command -v brew &> /dev/null; then
            brew install hudochenkov/sshpass/sshpass
        else
            echo "❌ Please install Homebrew first, or install sshpass manually"
            echo "Alternative: Use manual SSH with password: 6s0NeCqpAhDG"
            echo "ssh -o StrictHostKeyChecking=no azureuser@20.255.100.73"
            exit 1
        fi
    fi
fi

# Test Master VM
echo "Testing Master VM (20.255.100.73)..."
sshpass -p "6s0NeCqpAhDG" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null azureuser@20.255.100.73 "echo '✅ Master VM SSH successful'; hostname; uptime" 2>/dev/null

if [ $? -eq 0 ]; then
    echo "✅ Master VM SSH working!"
else
    echo "⚠️  Master VM SSH may still be starting up"
fi

echo ""
echo "Test one Slave VM (52.175.36.139)..."
sshpass -p "6s0NeCqpAhDG" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null azureuser@52.175.36.139 "echo '✅ Slave VM 1 SSH successful'; hostname; uptime" 2>/dev/null

if [ $? -eq 0 ]; then
    echo "✅ Slave VM 1 SSH working!"
else
    echo "⚠️  Slave VM 1 SSH may still be starting up"
fi

echo ""
echo "🎯 SSH connectivity test complete!"
echo ""
echo "💡 Manual SSH commands (if needed):"
echo "ssh -o StrictHostKeyChecking=no azureuser@20.255.100.73"
echo "Password: 6s0NeCqpAhDG"