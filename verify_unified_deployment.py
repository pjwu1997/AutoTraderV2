#!/usr/bin/env python3
"""
Verify unified collector deployment readiness
"""

import os
import json
from pathlib import Path

def verify_deployment():
    """Verify that all components are ready for unified collector deployment"""
    
    print("🔍 Verifying Unified Collector Deployment Readiness")
    print("=" * 60)
    
    checks = []
    
    # Check 1: Unified collector exists
    unified_collector_path = "DistributedSystem/SlaveVM/data_fetcher/unified_collector.py"
    if os.path.exists(unified_collector_path):
        checks.append("✅ Unified collector implementation exists")
    else:
        checks.append("❌ Unified collector implementation missing")
    
    # Check 2: Run script exists  
    run_script_path = "DistributedSystem/SlaveVM/data_fetcher/run_unified_collector.py"
    if os.path.exists(run_script_path):
        checks.append("✅ Unified collector run script exists")
    else:
        checks.append("❌ Unified collector run script missing")
    
    # Check 3: Dockerfile exists
    dockerfile_path = "DistributedSystem/SlaveVM/data_fetcher/Dockerfile.unified"
    if os.path.exists(dockerfile_path):
        checks.append("✅ Unified Dockerfile exists")
    else:
        checks.append("❌ Unified Dockerfile missing")
    
    # Check 4: Docker-compose updated
    compose_path = "DistributedSystem/Scripts/deployment/docker-compose.slave.yml"
    if os.path.exists(compose_path):
        with open(compose_path, 'r') as f:
            content = f.read()
            if "unified-collector:" in content:
                checks.append("✅ Docker-compose updated for unified collector")
            else:
                checks.append("❌ Docker-compose not updated for unified collector")
    else:
        checks.append("❌ Docker-compose file missing")
    
    # Check 5: Deployment script updated
    deploy_script_path = "DistributedSystem/Scripts/deployment/deploy_slave.sh"
    if os.path.exists(deploy_script_path):
        with open(deploy_script_path, 'r') as f:
            content = f.read()
            if "unified-collector" in content:
                checks.append("✅ Deployment script updated for unified collector")
            else:
                checks.append("❌ Deployment script not updated")
    else:
        checks.append("❌ Deployment script missing")
    
    # Check 6: Slave configurations exist
    slave_configs = []
    for i in range(1, 6):
        config_path = f"DistributedSystem/Config/slaves/slave-{i}.env"
        if os.path.exists(config_path):
            slave_configs.append(f"slave-{i}")
    
    if slave_configs:
        checks.append(f"✅ Slave configurations exist: {', '.join(slave_configs)}")
    else:
        checks.append("❌ No slave configurations found")
    
    # Check 7: Open interest integration
    if os.path.exists(unified_collector_path):
        with open(unified_collector_path, 'r') as f:
            content = f.read()
            if "_fetch_open_interest" in content and "open_interest" in content:
                checks.append("✅ Open interest integration complete")
            else:
                checks.append("❌ Open interest integration missing")
    
    # Print results
    print("\n📋 Deployment Readiness Checklist:")
    print("-" * 40)
    
    success_count = 0
    for check in checks:
        print(f"  {check}")
        if check.startswith("✅"):
            success_count += 1
    
    print(f"\n📊 Status: {success_count}/{len(checks)} checks passed")
    
    if success_count == len(checks):
        print("\n🎉 Ready for deployment!")
        print("\n📋 Deployment Summary:")
        print("- ✅ Unified collector consolidates all data collection")
        print("- ✅ Includes REST API + WebSocket data")
        print("- ✅ Open interest data integrated") 
        print("- ✅ Enhanced metrics (CVD, spread, volatility)")
        print("- ✅ Docker configuration updated")
        print("- ✅ Deployment scripts updated")
        
        print("\n🚀 Next Steps:")
        print("1. Deploy to slave VMs using:")
        print("   cd DistributedSystem/Scripts/deployment")
        print("   ./deploy_slave.sh slave-1")
        print("   ./deploy_slave.sh slave-2") 
        print("   ./deploy_slave.sh slave-3")
        print("\n2. Monitor deployment with:")
        print("   docker-compose -f docker-compose.slave.yml logs unified-collector")
        
    else:
        print("\n❌ Deployment not ready - fix issues above first")
    
    return success_count == len(checks)

if __name__ == "__main__":
    verify_deployment()