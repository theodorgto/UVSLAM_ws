import rclpy
from rclpy.node import Node
from builtin_interfaces.msg import Time
from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory

import argparse
import sys
import heapq

class MCAPPublisher(Node):
    def __init__(self, file_list, topics, start_realtime=True):
        super().__init__('mcap_merger_publisher')
        self.pub_map = {}
        self.messages = []
        for path in file_list:
            with open(path, 'rb') as f:
                reader = make_reader(f, decoder_factories=[DecoderFactory()])
                for schema, channel, message, ros_msg in reader.iter_decoded_messages(topics=topics):
                    ts = message.publish_time
                    heapq.heappush(self.messages, (ts, channel.topic, ros_msg))

        self.start_time = None
        self.start_realtime = start_realtime
        self.timer = self.create_timer(0.001, self.timer_callback)

    def timer_callback(self):
        if not self.messages:
            self.get_logger().info('Finished publishing all messages.')
            rclpy.shutdown()
            return

        ts, topic, ros_msg = self.messages[0]

        # Lazy‐create publisher with the real message type
        if topic not in self.pub_map:
            msg_type = ros_msg.__class__
            self.pub_map[topic] = self.create_publisher(msg_type, topic, 10)

        if self.start_time is None:
            self.start_time = ts
            self.base_time = self.get_clock().now().nanoseconds

        now = (self.get_clock().now().nanoseconds
               if self.start_realtime else ts)
        elapsed = now - self.base_time if self.start_realtime else 0
        target_offset = ts - self.start_time

        if elapsed >= target_offset:
            heapq.heappop(self.messages)
            ros_msg.header.stamp = Time(sec=ts // 10**9, nanosec=ts % 10**9)
            self.pub_map[topic].publish(ros_msg)
            self.get_logger().info(f"Published on {topic} at {ts}")


def main():
    parser = argparse.ArgumentParser(description='Merge two MCAP files and replay topics by timestamp')
    parser.add_argument('--files', nargs=2, required=True, help='Two MCAP file paths')
    parser.add_argument('--topics', nargs='+', required=True, help='List of topics to publish')
    parser.add_argument('--no-realtime', action='store_true', help='Publish without real-time delay')
    args = parser.parse_args()

    rclpy.init()
    node = MCAPPublisher(args.files, args.topics, start_realtime=not args.no_realtime)
    rclpy.spin(node)
    node.destroy_node()
    rclpy.shutdown()
