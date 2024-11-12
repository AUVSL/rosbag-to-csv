from setuptools import find_packages, setup

package_name = 'rosbag_to_csv'

setup(
    name=package_name,
    version='0.0.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
        ('share/' + package_name, ['config.yaml']),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='alex',
    maintainer_email='apgisi11@gmail.com',
    description='TODO: Package description',
    license='TODO: License declaration',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'data_recorder = rosbag_to_csv.data_recorder_node:main'
        ],
    },
)
