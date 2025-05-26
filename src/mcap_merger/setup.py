from setuptools import setup

package_name = 'mcap_merger'

setup(
    name=package_name,
    version='0.0.0',
    packages=[package_name],
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
    ],
    install_requires=[
        'setuptools',
        'mcap',
        'mcap-ros2-support'
    ],
    zip_safe=True,
    maintainer='Your Name',
    maintainer_email='you@example.com',
    description='Merge and replay MCAP topics in ROS 2',
    license='Apache License 2.0',
    entry_points={
        'console_scripts': [
            'mcap_merger_node = mcap_merger.mcap_merger_node:main',
        ],
    },
)
