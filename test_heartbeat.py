#!/usr/bin/env python3
"""
Test script for heartbeat system
"""

import time
import requests
from utils.background_job_manager import BackgroundJobManager

def test_heartbeat():
    """Test the heartbeat system"""
    print("🧪 Testing Heartbeat System")
    print("=" * 50)
    
    # Create job manager
    job_manager = BackgroundJobManager(max_workers=1, db_path="test_heartbeat.db")
    
    # Get initial status
    status = job_manager.get_heartbeat_status()
    print(f"Initial status: {status}")
    
    # Enable heartbeat
    job_manager.enable_heartbeat(True)
    status = job_manager.get_heartbeat_status()
    print(f"After enabling: {status}")
    
    # Test for 30 seconds
    print("\n💓 Testing heartbeat for 30 seconds...")
    for i in range(6):  # 6 iterations of 5 seconds each
        time.sleep(5)
        status = job_manager.get_heartbeat_status()
        print(f"  {i+1}/6 - Heartbeat running: {status['running']}")
    
    # Disable heartbeat
    job_manager.enable_heartbeat(False)
    status = job_manager.get_heartbeat_status()
    print(f"After disabling: {status}")
    
    # Cleanup
    job_manager.stop()
    print("\n✅ Test completed!")

if __name__ == "__main__":
    test_heartbeat()
