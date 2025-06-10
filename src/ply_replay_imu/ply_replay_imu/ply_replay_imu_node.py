import rclpy
from rclpy.node import Node
from sensor_msgs.msg import PointCloud2, PointField, Imu
from builtin_interfaces.msg import Time
from std_msgs.msg import Header

import numpy as np
import open3d as o3d
import csv
import os
import struct


def o3d_to_pointcloud2(pcd: o3d.geometry.PointCloud,
                       stamp: Time,
                       frame_id: str = "os_sensor") -> PointCloud2:
    points = np.asarray(pcd.points) * 10 # convert to cm
    colors = np.asarray(pcd.colors)
    has_color = (colors.shape[0] == points.shape[0])



    fields = [
        PointField(name='x', offset=0,  datatype=PointField.FLOAT32, count=1),
        PointField(name='z', offset=4,  datatype=PointField.FLOAT32, count=1),
        PointField(name='y', offset=8,  datatype=PointField.FLOAT32, count=1),
    ]
    point_step = 12
    if has_color:
        fields.append(PointField(name='rgb', offset=12,
                       datatype=PointField.FLOAT32, count=1))
        point_step = 16

    buf = bytearray()
    for i, pt3d in enumerate(points):
        buf.extend(struct.pack('fff', *pt3d))
        if has_color:
            r, g, b = (int(c*255) for c in colors[i])
            rgb_packed = (r << 16) | (g << 8) | b
            buf.extend(struct.pack('f',
                       struct.unpack('f', struct.pack('I', rgb_packed))[0]))

    msg = PointCloud2()
    msg.header = Header(stamp=stamp, frame_id=frame_id)
    msg.height = 1
    msg.width = len(points)
    msg.fields = fields
    msg.is_bigendian = False
    msg.point_step = point_step
    msg.row_step = point_step * len(points)
    msg.is_dense = False
    msg.data = bytes(buf)
    return msg

def crop_box(pcd, xmin, xmax, ymin, ymax, zmin, zmax):
    """
    Keep only points within the axis-aligned box [xmin,xmax]×[ymin,ymax]×[zmin,zmax].
    """

    pts = np.asarray(pcd.points)
    mask = (
        (pts[:, 0] >= xmin) & (pts[:, 0] <= xmax) &
        (pts[:, 1] >= ymin) & (pts[:, 1] <= ymax) &
        (pts[:, 2] >= zmin) & (pts[:, 2] <= zmax)
    )
    indices = np.where(mask)[0]
    return pcd.select_by_index(indices)

class SensorPublisher(Node):
    def __init__(self, pc_csv, pcd_dir, imu_csv, downsample_pc=1.0 ,imu_frame_id="imu_link"):
        super().__init__('sensor_publisher')

        # Publishers
        self.pc_pub = self.create_publisher(PointCloud2,
                                            '/os_cloud_node/points', 10)
        self.imu_pub = self.create_publisher(Imu,
                                             '/os_cloud_node/imu', 50)

        # Load point-cloud schedule
        self.pc_frames = []  # list of (ts_ns, ply_path)
        with open(pc_csv, 'r') as f:
            reader = csv.reader(f)
            # discard first line
            next(reader)
            for row in reader:
                frame_idx, ts = row[0], row[1]
                fname = f"frame_{int(frame_idx):04d}.ply"  # ensure consistent naming
                # self.get_logger().info(f"Loading PCD: {ts} -> {fname}")
                ts_ns = int(ts)
                path = os.path.join(pcd_dir, fname)
                self.pc_frames.append((ts_ns, path))
        
        # Downsample pointclouds if requested
        if downsample_pc < 1.0:
            self.get_logger().info(f"Downsampling pointclouds by {downsample_pc:.2f}")
            self.pc_frames = self.pc_frames[::int(1/downsample_pc)]
        self.pc_frames.sort(key=lambda x: x[0])
        self.pc_idx = 0

        # Load IMU schedule
        self.imu_msgs = []  # list of (ts_ns, imu_msg)
        with open(imu_csv, 'r') as f:
            reader = csv.reader(f)
            # discard first line
            next(reader)
            for row in reader:
                # expected columns: ts_ns, qx, qy, qz, qw,
                #                   avx, avy, avz,
                #                   lax, lay, laz
                ts_ns = int(row[0])
                imu = Imu()
                imu.header.frame_id = imu_frame_id
                imu.header.stamp.sec = ts_ns // 10**9
                imu.header.stamp.nanosec = ts_ns % 10**9

                imu.orientation.x = float(row[1])
                imu.orientation.y = float(row[2])
                imu.orientation.z = float(row[3])
                imu.orientation.w = float(row[4])

                imu.angular_velocity.x = float(row[5])
                imu.angular_velocity.y = float(row[6])
                imu.angular_velocity.z = float(row[7])

                imu.linear_acceleration.x = float(row[8])
                imu.linear_acceleration.y = float(row[9])
                imu.linear_acceleration.z = float(row[10])

                

                self.imu_msgs.append((ts_ns, imu))

        self.imu_msgs.sort(key=lambda x: x[0])


        # ---------------------------------------------------
        # Compute mean bias on linear accel X & Y, then remove
        # ---------------------------------------------------
        # extract all x,y accel values
        # lax_vals = [msg.linear_acceleration.x for _, msg in self.imu_msgs]
        # lay_vals = [msg.linear_acceleration.y for _, msg in self.imu_msgs]

        # # compute mean bias
        # bias_x = sum(lax_vals) / len(lax_vals)
        # bias_y = sum(lay_vals) / len(lay_vals)
        # self.get_logger().info(f"Computed accel bias: x={bias_x:.6f}, y={bias_y:.6f}")

        # # subtract bias from each message
        # for _, msg in self.imu_msgs:
        #     msg.linear_acceleration.x -= bias_x
        #     msg.linear_acceleration.y -= bias_y

        self.imu_idx = 0



        # Timer at 1 ms resolution
        self.timer = self.create_timer(0.001, self.on_timer)

    def on_timer(self):
        now = self.get_clock().now().nanoseconds

        # Find next pending timestamps (or +inf if done)
        next_pc_ts = (self.pc_frames[self.pc_idx][0]
                      if self.pc_idx < len(self.pc_frames)
                      else float('inf'))
        next_imu_ts = (self.imu_msgs[self.imu_idx][0]
                       if self.imu_idx < len(self.imu_msgs)
                       else float('inf'))

        # If neither has any left, we're done
        if next_pc_ts == float('inf') or next_imu_ts == float('inf'):
            self.get_logger().info("Finished all data, shutting down.")
            rclpy.shutdown()
            return

        # Publish whichever is next and whose timestamp has passed
        if now >= next_pc_ts and next_pc_ts <= next_imu_ts:
            ts_ns, plyfile = self.pc_frames[self.pc_idx]
            pcd = o3d.io.read_point_cloud(plyfile)

            # ### filter background points
            # distance_threshold = 1.3
            # pcd = pcd.select_by_index(np.where(np.asarray(pcd.points)[:, 2] < distance_threshold)[0])
            # self.get_logger().info(f"Filtered PCD: {len(pcd.points)} points within distance < {distance_threshold} ")

            # ### crop to box
            XMIN, XMAX = -0.4, 0.6
            YMIN, YMAX = -1.0, 1.0
            ZMIN, ZMAX = -0.3, 1.3

            pcd = crop_box(pcd, XMIN, XMAX, YMIN, YMAX, ZMIN, ZMAX)
            self.get_logger().info(f"Cropped PCD: {len(pcd.points)} points within box ")

            stamp = Time(sec=ts_ns // 10**9, nanosec=ts_ns % 10**9)
            msg = o3d_to_pointcloud2(pcd, stamp)

            self.pc_pub.publish(msg)
            self.get_logger().info(f"PCD #{self.pc_idx} @ {ts_ns} ns → {plyfile}")
            self.pc_idx += 1

        elif now >= next_imu_ts:
            ts_ns, imu_msg = self.imu_msgs[self.imu_idx]
            # header.stamp was already set on load
            self.imu_pub.publish(imu_msg)
            self.get_logger().info(f"IMU   #{self.imu_idx} @ {ts_ns} ns")
            self.imu_idx += 1

        # else: next event hasn't arrived yet, wait for future timer ticks


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="ROS2 node publishing synced pointclouds + IMU")
    parser.add_argument('--pc_csv',   required=True,
                        help='CSV with columns: ts_ns, ply_filename')
    parser.add_argument('--pcd_dir',  required=True,
                        help='Directory containing PLY files')
    parser.add_argument('--imu_csv',  required=True,
                        help='CSV with columns: ts_ns, qx, qy, qz, qw, '
                             'avx, avy, avz, lax, lay, laz')
    parser.add_argument('--downsample_pc', type=float, default=1.0,
                        help='Downsample pointclouds by this factor (1.0 = no downsample)')
    parser.add_argument('--imu_frame_id', default='imu_link',
                        help='frame_id to stamp IMU messages')
    args = parser.parse_args()

    rclpy.init()
    node = SensorPublisher(args.pc_csv,
                           args.pcd_dir,
                           args.imu_csv,
                           args.downsample_pc,
                           args.imu_frame_id)
    rclpy.spin(node)
    node.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    self.get_logger().info("Starting ply_replay_imu_node")
    main()
