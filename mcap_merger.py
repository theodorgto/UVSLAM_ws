#!/usr/bin/env python3
"""
MCAP File Merger with Timestamp Synchronization

This script merges two ROS2 MCAP files while synchronizing their timestamps
based on the header timestamps of the files.
"""

import argparse
import sys
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any
import logging

try:
    from mcap.reader import make_reader
    from mcap.writer import Writer
    from mcap_ros2.reader import read_ros2_messages
    import mcap.well_known as well_known
except ImportError:
    print("Error: Required MCAP libraries not found.")
    print("Please install them with: pip install mcap mcap-ros2-support")
    sys.exit(1)


def get_file_timestamps(mcap_file: Path) -> Tuple[Optional[int], Optional[int]]:
    """
    Extract the first and last timestamps from an MCAP file.
    
    Args:
        mcap_file: Path to the MCAP file
        
    Returns:
        Tuple of (first_timestamp, last_timestamp) in nanoseconds
    """
    first_ts = None
    last_ts = None
    
    try:
        with open(mcap_file, "rb") as f:
            reader = make_reader(f)
            summary = reader.get_summary()
            
            if summary and summary.statistics:
                first_ts = summary.statistics.message_start_time
                last_ts = summary.statistics.message_end_time
            else:
                # Fallback: read messages to find timestamps
                logging.info("No summary statistics found, scanning messages...")
                for msg in read_ros2_messages(mcap_file):
                    if first_ts is None:
                        first_ts = msg.message.log_time
                    last_ts = msg.message.log_time
                    
    except Exception as e:
        logging.error(f"Error reading timestamps from {mcap_file}: {e}")
        return None, None
        
    return first_ts, last_ts


def collect_messages_with_offset(mcap_file: Path, time_offset: int) -> List[Dict[str, Any]]:
    """
    Collect all messages from an MCAP file with timestamp offset applied.
    
    Args:
        mcap_file: Path to the MCAP file
        time_offset: Nanoseconds to add to each timestamp
        
    Returns:
        List of message dictionaries with adjusted timestamps
    """
    messages = []
    
    try:
        for msg in read_ros2_messages(mcap_file):
            adjusted_log_time = msg.message.log_time + time_offset
            adjusted_publish_time = msg.message.publish_time + time_offset
            
            messages.append({
                'topic': msg.channel.topic,
                'log_time': adjusted_log_time,
                'publish_time': adjusted_publish_time,
                'data': msg.message.data,
                'schema': msg.schema,
                'channel': msg.channel,
                'message': msg.message
            })
            
    except Exception as e:
        logging.error(f"Error reading messages from {mcap_file}: {e}")
        raise
        
    return messages


def merge_mcap_files(file1: Path, file2: Path, output_file: Path, sync_method: str = "earliest"):
    """
    Merge two MCAP files with synchronized timestamps.
    
    Args:
        file1: Path to first MCAP file
        file2: Path to second MCAP file  
        output_file: Path for merged output file
        sync_method: Synchronization method ("earliest", "latest", or "file1")
    """
    
    # Get timestamps from both files
    logging.info("Analyzing input files...")
    file1_start, file1_end = get_file_timestamps(file1)
    file2_start, file2_end = get_file_timestamps(file2)
    
    if file1_start is None or file2_start is None:
        raise ValueError("Could not determine timestamps from input files")
    
    logging.info(f"File 1 time range: {file1_start} to {file1_end}")
    logging.info(f"File 2 time range: {file2_start} to {file2_end}")
    
    # Calculate time offset based on sync method
    if sync_method == "earliest":
        # Align both files to start at the same time as the earliest file
        reference_time = min(file1_start, file2_start)
        offset1 = reference_time - file1_start
        offset2 = reference_time - file2_start
    elif sync_method == "latest":
        # Align both files to start at the same time as the latest file
        reference_time = max(file1_start, file2_start)
        offset1 = reference_time - file1_start
        offset2 = reference_time - file2_start
    elif sync_method == "file1":
        # Align file2 to start at the same time as file1
        offset1 = 0
        offset2 = file1_start - file2_start
    else:
        raise ValueError(f"Unknown sync method: {sync_method}")
    
    logging.info(f"Time offsets: File1={offset1}, File2={offset2}")
    
    # Collect all messages from both files with offsets applied
    logging.info("Collecting messages from first file...")
    messages1 = collect_messages_with_offset(file1, offset1)
    
    logging.info("Collecting messages from second file...")
    messages2 = collect_messages_with_offset(file2, offset2)
    
    # Combine and sort all messages by timestamp
    logging.info("Merging and sorting messages...")
    all_messages = messages1 + messages2
    all_messages.sort(key=lambda x: x['log_time'])
    
    logging.info(f"Total messages to write: {len(all_messages)}")
    
    # Write merged file
    logging.info(f"Writing merged file to {output_file}")
    
    with open(output_file, "wb") as f:
        writer = Writer(f)
        
        # Keep track of schemas and channels we've already registered
        registered_schemas = {}
        registered_channels = {}
        
        try:
            for msg_data in all_messages:
                schema = msg_data['schema']
                channel = msg_data['channel']
                
                # Register schema if not already done
                schema_id = schema.id
                if schema_id not in registered_schemas:
                    writer.add_schema(
                        name=schema.name,
                        encoding=schema.encoding,
                        data=schema.data
                    )
                    registered_schemas[schema_id] = schema
                
                # Register channel if not already done
                channel_id = channel.id
                if channel_id not in registered_channels:
                    writer.add_channel(
                        topic=channel.topic,
                        message_encoding=channel.message_encoding,
                        metadata=channel.metadata,
                        schema_id=schema_id
                    )
                    registered_channels[channel_id] = channel
                
                # Write the message with adjusted timestamp
                writer.add_message(
                    channel_id=channel_id,
                    log_time=msg_data['log_time'],
                    data=msg_data['data'],
                    publish_time=msg_data['publish_time']
                )
                
        finally:
            writer.finish()
    
    logging.info("Merge completed successfully!")
    
    # Print summary
    final_start, final_end = get_file_timestamps(output_file)
    if final_start and final_end:
        logging.info(f"Merged file time range: {final_start} to {final_end}")
        logging.info(f"Total duration: {(final_end - final_start) / 1e9:.2f} seconds")


def main():
    parser = argparse.ArgumentParser(
        description="Merge two ROS2 MCAP files with synchronized timestamps"
    )
    parser.add_argument("file1", type=Path, help="First MCAP file")
    parser.add_argument("file2", type=Path, help="Second MCAP file")
    parser.add_argument("output", type=Path, help="Output merged MCAP file")
    parser.add_argument(
        "--sync-method",
        choices=["earliest", "latest", "file1"],
        default="earliest",
        help="Timestamp synchronization method (default: earliest)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.INFO if args.verbose else logging.WARNING
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Validate input files
    if not args.file1.exists():
        print(f"Error: File {args.file1} does not exist")
        sys.exit(1)
    
    if not args.file2.exists():
        print(f"Error: File {args.file2} does not exist")
        sys.exit(1)
    
    # Create output directory if needed
    args.output.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        merge_mcap_files(args.file1, args.file2, args.output, args.sync_method)
        print(f"Successfully merged files into {args.output}")
        
    except Exception as e:
        logging.error(f"Error during merge: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()