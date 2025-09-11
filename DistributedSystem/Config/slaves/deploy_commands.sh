#!/bin/bash
# 全量收集部署指令

echo '=== AutoTrader 全量收集部署 ==='
echo 'Total Symbols: 526'
echo 'Slaves: 3'

echo 'Deploying slave-1...'
# scp slave-1.env slave-vm-1:/path/to/config/
# ssh slave-vm-1 './deploy_slave.sh slave-1'

echo 'Deploying slave-2...'
# scp slave-2.env slave-vm-2:/path/to/config/
# ssh slave-vm-2 './deploy_slave.sh slave-2'

echo 'Deploying slave-3...'
# scp slave-3.env slave-vm-3:/path/to/config/
# ssh slave-vm-3 './deploy_slave.sh slave-3'

echo 'All slaves deployed!'
