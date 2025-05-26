import sys
if sys.prefix == '/usr':
    sys.real_prefix = sys.prefix
    sys.prefix = sys.exec_prefix = '/nvme_ros2_ws/src/mcap_merger/install/mcap_merger'
