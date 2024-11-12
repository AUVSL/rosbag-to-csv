# rosbag-to-csv
Stream selected topics from a rosbag into a csv. Just set the config.yaml appropriately.

## Use
```
 ros2 run rosbag_to_csv data_recorder --ros-args -p config_file:=/path/to/rosbag_to_csv_ws/src/rosbag_to_csv/config.yaml -p interval:=0.1 -p output_csv:=/path/to/output.csv
```
