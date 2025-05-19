import rclpy
from rclpy.node import Node
from sensor_msgs.msg import PointCloud2, PointField
from builtin_interfaces.msg import Time

import numpy as np
import open3d as o3d
import csv
import os
import sys

from std_msgs.msg import Header
import struct

def o3d_to_pointcloud2(pcd: o3d.geometry.PointCloud, stamp: Time, frame_id: str = "os_sensor") -> PointCloud2:
    points = np.asarray(pcd.points)
    colors = np.asarray(pcd.colors)

    has_color = colors.shape[0] == points.shape[0]

    fields = [
        PointField(name='x', offset=0,  datatype=PointField.FLOAT32, count=1),
        PointField(name='y', offset=4,  datatype=PointField.FLOAT32, count=1),
        PointField(name='z', offset=8,  datatype=PointField.FLOAT32, count=1)
    ]

    point_step = 12
    if has_color:
        fields.append(PointField(name='rgb', offset=12, datatype=PointField.FLOAT32, count=1))
        point_step = 16

    data = bytearray()
    for i in range(len(points)):
        pt = struct.pack('fff', *points[i])
        if has_color:
            rgb = (int(colors[i][0]*255) << 16) | (int(colors[i][1]*255) << 8) | int(colors[i][2]*255)
            pt += struct.pack('f', struct.unpack('f', struct.pack('I', rgb))[0])
        data.extend(pt)

    msg = PointCloud2()
    msg.header = Header(stamp=stamp, frame_id=frame_id)
    msg.height = 1
    msg.width = len(points)
    msg.fields = fields
    msg.is_bigendian = False
    msg.point_step = point_step
    msg.row_step = point_step * len(points)
    msg.is_dense = False
    msg.data = bytes(data)

    return msg


class PLYPublisher(Node):
    def __init__(self, csv_file, pcd_dir):
        super().__init__('ply_publisher')
        self.publisher_ = self.create_publisher(PointCloud2, '/os_cloud_node/points', 10)

        self.frames = []
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                stamp_ns = int(row[0])
                fname = row[1]
                path = os.path.join(pcd_dir, fname)
                self.frames.append((stamp_ns, path))

        self.index = 0
        self.timer = self.create_timer(0.001, self.timer_callback)  # 1ms resolution

    def timer_callback(self):
        if self.index >= len(self.frames):
            self.get_logger().info("Finished publishing all frames.")
            rclpy.shutdown()
            return

        now_ros_time = self.get_clock().now().nanoseconds
        frame_ns, ply_path = self.frames[self.index]

        if now_ros_time >= frame_ns:
            pcd = o3d.io.read_point_cloud(ply_path)
            stamp = Time(sec=frame_ns // 10**9, nanosec=frame_ns % 10**9)
            msg = o3d_to_pointcloud2(pcd, stamp)
            self.publisher_.publish(msg)
            self.get_logger().info(f"Published {ply_path}")
            self.index += 1


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', required=True, help='CSV file with timestamp and filename')
    parser.add_argument('--pcd_dir', required=True, help='Directory with .ply files')
    args = parser.parse_args()

    rclpy.init()
    node = PLYPublisher(args.csv, args.pcd_dir)
    rclpy.spin(node)
    node.destroy_node()
    rclpy.shutdown()

