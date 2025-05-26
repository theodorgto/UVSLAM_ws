#!/usr/bin/env python3

import argparse
from pathlib import Path
import sys

from rosbags.rosbag2 import Reader, Writer
from rosbags.serde import deserialize_cdr, serialize_cdr
from rosbags.typesys import get_types_from_msg, register_types

# --- Configuration ---
DEFAULT_PCL_INPUT_TOPIC_NAME = '/os_cloud_node/points' # Placeholder: PLEASE VERIFY AND CHANGE THIS

DEFAULT_IMU_INPUT_BAG_PATH = '/mnt/nvme/data/split/imu'
DEFAULT_PCL_INPUT_BAG_PATH = '/mnt/nvme/data/split/pcl'
DEFAULT_OUTPUT_BAG_PATH = './merged_synced_bag' 

IMU_INPUT_TOPIC_NAME = '/bluerov2/mavros/imu/data'
PCL_OUTPUT_TOPIC_NAME = '/os_cloud_node/points'
IMU_OUTPUT_TOPIC_NAME = '/os_cloud_node/imu'

# --- Helper Functions ---

def stamp_to_nanoseconds(stamp):
    """Converts a ROS Time message (stamp) to total nanoseconds."""
    return stamp.sec * 1_000_000_000 + stamp.nanosec

def get_msg_defs():
    """
    Manually define message definitions for sensor_msgs/Imu and sensor_msgs/PointCloud2.
    Returns a dictionary mapping message type names to TypeDef objects.
    """
    custom_types = {}
    
    # Note: The definition strings (e.g., header_def_str) are used later for add_connection.
    # std_msgs/Time (dependency)
    time_def_str = "int32 sec\nuint32 nanosec"
    custom_types['std_msgs/msg/Time'] = get_types_from_msg(time_def_str, 'std_msgs/msg/Time')

    # std_msgs/Header
    header_def_str = """std_msgs/Time stamp\nstring frame_id"""
    custom_types['std_msgs/msg/Header'] = get_types_from_msg(header_def_str, 'std_msgs/msg/Header')

    # geometry_msgs/Quaternion (dependency for Imu)
    quaternion_def_str = "float64 x\nfloat64 y\nfloat64 z\nfloat64 w"
    custom_types['geometry_msgs/msg/Quaternion'] = get_types_from_msg(quaternion_def_str, 'geometry_msgs/msg/Quaternion')

    # geometry_msgs/Vector3 (dependency for Imu)
    vector3_def_str = "float64 x\nfloat64 y\nfloat64 z"
    custom_types['geometry_msgs/msg/Vector3'] = get_types_from_msg(vector3_def_str, 'geometry_msgs/msg/Vector3')
    
    # sensor_msgs/Imu
    imu_def_str = """
std_msgs/Header header
geometry_msgs/Quaternion orientation
float64[9] orientation_covariance
geometry_msgs/Vector3 angular_velocity
float64[9] angular_velocity_covariance
geometry_msgs/Vector3 linear_acceleration
float64[9] linear_acceleration_covariance
"""
    custom_types['sensor_msgs/msg/Imu'] = get_types_from_msg(imu_def_str, 'sensor_msgs/msg/Imu')

    # sensor_msgs/PointField (dependency for PointCloud2)
    point_field_def_str = """
uint8 INT8=1
uint8 UINT8=2
uint8 INT16=3
uint8 UINT16=4
uint8 INT32=5
uint8 UINT32=6
uint8 FLOAT32=7
uint8 FLOAT64=8
string name
uint32 offset
uint8 datatype
uint32 count
"""
    custom_types['sensor_msgs/msg/PointField'] = get_types_from_msg(point_field_def_str, 'sensor_msgs/msg/PointField')

    # sensor_msgs/PointCloud2
    pcl2_def_str = """
std_msgs/Header header
uint32 height
uint32 width
sensor_msgs/PointField[] fields
bool is_bigendian
uint32 point_step
uint32 row_step
uint8[] data
bool is_dense
"""
    custom_types['sensor_msgs/msg/PointCloud2'] = get_types_from_msg(pcl2_def_str, 'sensor_msgs/msg/PointCloud2')
    
    return custom_types


def main():
    parser = argparse.ArgumentParser(description="Merge and synchronize IMU and PCL ROS2 bags based on header timestamps.")
    parser.add_argument('--imu_bag', type=str, default=DEFAULT_IMU_INPUT_BAG_PATH, help="Path to the input IMU bag directory.")
    parser.add_argument('--pcl_bag', type=str, default=DEFAULT_PCL_INPUT_BAG_PATH, help="Path to the input PCL bag directory.")
    parser.add_argument('--output_bag', type=str, default=DEFAULT_OUTPUT_BAG_PATH, help="Path for the output merged bag directory.")
    parser.add_argument('--pcl_input_topic', type=str, default=DEFAULT_PCL_INPUT_TOPIC_NAME, help="Topic name of PointCloud2 messages in the PCL bag.")
    
    args = parser.parse_args()

    imu_bag_path = Path(args.imu_bag)
    pcl_bag_path = Path(args.pcl_bag)
    output_bag_path = Path(args.output_bag)
    pcl_input_topic_name = args.pcl_input_topic

    if not imu_bag_path.exists() or not imu_bag_path.is_dir():
        print(f"Error: IMU bag path does not exist or is not a directory: {imu_bag_path}")
        sys.exit(1)
    if not pcl_bag_path.exists() or not pcl_bag_path.is_dir():
        print(f"Error: PCL bag path does not exist or is not a directory: {pcl_bag_path}")
        sys.exit(1)

    # Get message definitions and register them
    # The TypeDef objects returned by get_types_from_msg contain a .definition attribute
    # which is the raw string definition.
    msg_defs_map = get_msg_defs()
    try:
        register_types(msg_defs_map)
        print("Manually registered message definitions.")
    except Exception as e:
        print(f"Could not manually register message definitions (this might be OK if rosbags finds them automatically): {e}")


    print(f"Processing PCL bag: {pcl_bag_path}")
    print(f"Searching for PCL topic: {pcl_input_topic_name}")

    pcl_messages_to_write = []
    min_pcl_header_stamp_ns = float('inf')
    max_pcl_header_stamp_ns = float('-inf')
    pcl_msg_type_name = None # Will store the string like "sensor_msgs/msg/PointCloud2"
    pcl_connection_reader = None # Connection object from the reader

    try:
        with Reader(pcl_bag_path) as pcl_reader:
            for conn in pcl_reader.connections:
                if conn.topic == pcl_input_topic_name:
                    pcl_connection_reader = conn
                    pcl_msg_type_name = conn.msgtype
                    break
            
            if not pcl_connection_reader:
                print(f"Error: PCL topic '{pcl_input_topic_name}' not found in {pcl_bag_path}.")
                print("Available topics in PCL bag:")
                for conn_debug in pcl_reader.connections:
                    print(f"  - {conn_debug.topic} (Type: {conn_debug.msgtype})")
                sys.exit(1)

            print(f"Found PCL topic '{pcl_input_topic_name}' with type '{pcl_msg_type_name}'. Reading messages...")

            message_count = 0
            for conn, timestamp, rawdata in pcl_reader.messages(connections=[pcl_connection_reader]):
                # Deserialize to access header.stamp
                # We need to ensure that the msg_defs_map contains the type definition for pcl_msg_type_name
                # for deserialize_cdr to work correctly if types are not found from system.
                if pcl_msg_type_name not in msg_defs_map:
                    print(f"Critical Error: PCL message type '{pcl_msg_type_name}' not found in our manual definitions. Cannot deserialize.")
                    # Attempt to deserialize anyway, relying on rosbags finding the type
                    # This part might fail if types aren't globally available and not in our map
                    try:
                        msg = deserialize_cdr(rawdata, pcl_msg_type_name)
                    except Exception as e_des:
                        print(f"Failed to deserialize PCL message of type {pcl_msg_type_name} (even after trying): {e_des}")
                        print("Ensure this type is defined in get_msg_defs() or available in your ROS environment.")
                        continue # Skip this message
                else:
                     msg = deserialize_cdr(rawdata, pcl_msg_type_name)


                if not hasattr(msg, 'header') or not hasattr(msg.header, 'stamp'):
                    print(f"Warning: PCL message on topic {conn.topic} at bag time {timestamp} lacks header.stamp. Skipping.")
                    continue
                
                header_stamp_ns = stamp_to_nanoseconds(msg.header.stamp)
                min_pcl_header_stamp_ns = min(min_pcl_header_stamp_ns, header_stamp_ns)
                max_pcl_header_stamp_ns = max(max_pcl_header_stamp_ns, header_stamp_ns)
                
                pcl_messages_to_write.append({
                    'header_stamp_ns': header_stamp_ns,
                    'raw_data': rawdata,
                    'original_bag_time_ns': timestamp
                })
                message_count +=1
            print(f"Read {message_count} PCL messages.")

    except Exception as e:
        print(f"Error processing PCL bag: {e}")
        sys.exit(1)

    if not pcl_messages_to_write:
        print("No PCL messages found or processed. Exiting.")
        sys.exit(1)

    pcl_duration_s = (max_pcl_header_stamp_ns - min_pcl_header_stamp_ns) / 1e9
    print(f"PCL data time window (based on header.stamp):")
    print(f"  Min PCL Header Stamp: {min_pcl_header_stamp_ns} ns ({min_pcl_header_stamp_ns / 1e9:.3f} s)")
    print(f"  Max PCL Header Stamp: {max_pcl_header_stamp_ns} ns ({max_pcl_header_stamp_ns / 1e9:.3f} s)")
    print(f"  Duration: {pcl_duration_s:.3f} s")

    if abs(pcl_duration_s - 60.0) > 15.0 : 
        print(f"Warning: PCL data duration ({pcl_duration_s:.2f}s) is not approximately 1 minute. The output bag will match this duration.")

    print(f"\nProcessing IMU bag: {imu_bag_path}")
    imu_messages_to_write = []
    imu_msg_type_name = None # Will store the string like "sensor_msgs/msg/Imu"
    imu_connection_reader = None

    try:
        with Reader(imu_bag_path) as imu_reader:
            for conn in imu_reader.connections:
                if conn.topic == IMU_INPUT_TOPIC_NAME:
                    imu_connection_reader = conn
                    imu_msg_type_name = conn.msgtype
                    break
            
            if not imu_connection_reader:
                print(f"Error: IMU topic '{IMU_INPUT_TOPIC_NAME}' not found in {imu_bag_path}.")
                print("Available topics in IMU bag:")
                for conn_debug in imu_reader.connections:
                    print(f"  - {conn_debug.topic} (Type: {conn_debug.msgtype})")
                sys.exit(1)
            
            print(f"Found IMU topic '{IMU_INPUT_TOPIC_NAME}' with type '{imu_msg_type_name}'. Reading and filtering messages...")
            
            message_count = 0
            filtered_count = 0
            for conn, timestamp, rawdata in imu_reader.messages(connections=[imu_connection_reader]):
                if imu_msg_type_name not in msg_defs_map:
                    print(f"Critical Error: IMU message type '{imu_msg_type_name}' not found in our manual definitions. Cannot deserialize.")
                    try:
                        msg = deserialize_cdr(rawdata, imu_msg_type_name)
                    except Exception as e_des:
                        print(f"Failed to deserialize IMU message of type {imu_msg_type_name} (even after trying): {e_des}")
                        print("Ensure this type is defined in get_msg_defs() or available in your ROS environment.")
                        continue
                else:
                    msg = deserialize_cdr(rawdata, imu_msg_type_name)

                if not hasattr(msg, 'header') or not hasattr(msg.header, 'stamp'):
                    print(f"Warning: IMU message on topic {conn.topic} at bag time {timestamp} lacks header.stamp. Skipping.")
                    continue

                header_stamp_ns = stamp_to_nanoseconds(msg.header.stamp)
                message_count +=1
                if min_pcl_header_stamp_ns <= header_stamp_ns <= max_pcl_header_stamp_ns:
                    imu_messages_to_write.append({
                        'header_stamp_ns': header_stamp_ns,
                        'raw_data': rawdata,
                        'original_bag_time_ns': timestamp
                    })
                    filtered_count +=1
            print(f"Read {message_count} IMU messages, {filtered_count} fall within PCL time window.")

    except Exception as e:
        print(f"Error processing IMU bag: {e}")
        sys.exit(1)

    if not imu_messages_to_write and not pcl_messages_to_write :
        print("No messages to write to output bag. Exiting.")
        sys.exit(0) 

    all_messages_sorted = []
    for pcl_msg_data in pcl_messages_to_write:
        all_messages_sorted.append({
            'topic_str': PCL_OUTPUT_TOPIC_NAME, # Store original topic string for mapping
            'timestamp_ns': pcl_msg_data['header_stamp_ns'],
            'raw_data': pcl_msg_data['raw_data'],
            'msgtype': pcl_msg_type_name 
        })
    
    for imu_msg_data in imu_messages_to_write:
         all_messages_sorted.append({
            'topic_str': IMU_OUTPUT_TOPIC_NAME, # Store original topic string for mapping
            'timestamp_ns': imu_msg_data['header_stamp_ns'],
            'raw_data': imu_msg_data['raw_data'],
            'msgtype': imu_msg_type_name
        })

    all_messages_sorted.sort(key=lambda m: m['timestamp_ns'])

    print(f"\nWriting output bag to: {output_bag_path}")
    try:
        # output_bag_path.mkdir(parents=True, exist_ok=True)
        # Specify bag version, e.g., 8 for ROS 2 Humble/Iron compatibility
        with Writer(output_bag_path, version=8) as writer:
            output_connection_ids = {} # Map topic string to writer's connection ID

            if pcl_msg_type_name and pcl_messages_to_write:
                pcl_typedef = msg_defs_map.get(pcl_msg_type_name)
                if not pcl_typedef:
                    print(f"Error: Could not find TypeDef for PCL type '{pcl_msg_type_name}' in msg_defs_map. Cannot create writer connection.")
                else:
                    connection_pcl = writer.add_connection(
                        PCL_OUTPUT_TOPIC_NAME, 
                        pcl_msg_type_name,
                        msgdef=pcl_typedef.definition # Provide the raw message definition string
                    )
                    output_connection_ids[PCL_OUTPUT_TOPIC_NAME] = connection_pcl.id
                    print(f"Added writer connection for PCL topic: {PCL_OUTPUT_TOPIC_NAME} (Type: {pcl_msg_type_name}), Conn ID: {connection_pcl.id}")
            
            if imu_msg_type_name and imu_messages_to_write:
                imu_typedef = msg_defs_map.get(imu_msg_type_name)
                if not imu_typedef:
                     print(f"Error: Could not find TypeDef for IMU type '{imu_msg_type_name}' in msg_defs_map. Cannot create writer connection.")
                else:
                    connection_imu = writer.add_connection(
                        IMU_OUTPUT_TOPIC_NAME, 
                        imu_msg_type_name,
                        msgdef=imu_typedef.definition # Provide the raw message definition string
                    )
                    output_connection_ids[IMU_OUTPUT_TOPIC_NAME] = connection_imu.id
                    print(f"Added writer connection for IMU topic: {IMU_OUTPUT_TOPIC_NAME} (Type: {imu_msg_type_name}), Conn ID: {connection_imu.id}")

            if not all_messages_sorted:
                print("No messages to write after sorting. Output bag will be empty but valid.")
            else:
                written_count = 0
                for msg_data in all_messages_sorted:
                    topic_key = msg_data['topic_str']
                    if topic_key in output_connection_ids:
                        writer.write(
                            output_connection_ids[topic_key], # Use the connection ID
                            msg_data['raw_data'], 
                            msg_data['timestamp_ns']
                        )
                        written_count +=1
                    else:
                        print(f"Warning: No output connection ID found for topic '{topic_key}'. Skipping message. This may happen if connection adding failed.")
                print(f"Wrote {written_count} messages to the output bag.")

    except Exception as e:
        print(f"Error writing output bag: {e}")
        sys.exit(1)

    print("\nScript finished successfully.")
    print(f"Output bag created at: {output_bag_path}")

if __name__ == '__main__':
    main()
