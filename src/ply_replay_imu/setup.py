from setuptools import setup

package_name = 'ply_replay_imu'

setup(
    name=package_name,
    version='0.1.0',
    packages=[package_name],
    data_files=[
    # 1) register package in the ament index
    ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
    # 2) install package.xml for downstream
    ('share/' + package_name, ['package.xml']),
    ],

    install_requires=['setuptools', 'open3d', 'numpy'],
    zip_safe=True,
    maintainer='Your Name',
    maintainer_email='you@example.com',
    description='Replay PLY pointclouds as ROS 2 messages.',
    entry_points={
        'console_scripts': [
            'ply_replay_imu_node = ply_replay_imu.ply_replay_imu_node:main',
        ],
    },
)

