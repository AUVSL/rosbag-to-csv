import rclpy
from rclpy.node import Node
import yaml
import csv
import threading
from rosidl_runtime_py.utilities import get_message
from rclpy.qos import QoSProfile
from ament_index_python import get_package_share_directory

class DataRecorderNode(Node):
    def __init__(self):
        super().__init__('data_recorder')

        # Declare and get parameters
        self.declare_parameter('config_file', 'config.yaml')
        self.declare_parameter('interval', 0.05)
        self.declare_parameter('output_csv', 'output.csv')

        config_file = self.get_parameter('config_file').value
        interval = self.get_parameter('interval').value
        self.output_csv = self.get_parameter('output_csv').value

        # Load configuration
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)

        self.recorded_subscriptions = {}
        self.latest_messages = {}
        self.fields = []
        self.data_lock = threading.Lock()
        self.data = []

        # Set up subscribers based on the config file
        for sub in config['subscriptions']:
            topic_name = sub['topic_name']
            message_type_str = sub['message_type']
            fields = sub['fields']

            # Dynamically import the message type
            msg_module = get_message(message_type_str)
            if msg_module is None:
                self.get_logger().error(f"Could not import message type {message_type_str}")
                continue

            # Create a subscription for each topic
            subscription = self.create_subscription(
                msg_module,
                topic_name,
                self.create_callback(topic_name),
                QoSProfile(depth=10)
            )
            self.recorded_subscriptions[topic_name] = {
                'subscription': subscription,
                'message_type': msg_module,
                'fields': fields
            }
            self.latest_messages[topic_name] = None

            # Collect field information
            for field in fields:
                field_name = field['name']
                field_path = field['field_path']
                self.fields.append({
                    'name': field_name,
                    'topic_name': topic_name,
                    'field_path': field_path
                })

        # Set up a timer for recording data at fixed intervals
        self.timer = self.create_timer(interval, self.record_data)

    def create_callback(self, topic_name):
        def callback(msg):
            self.message_callback(msg, topic_name)
        return callback

    def message_callback(self, msg, topic_name):
        with self.data_lock:
            self.latest_messages[topic_name] = msg

    def get_field_value(self, msg, field_path):
        attrs = field_path.split('.')
        value = msg
        try:
            for attr in attrs:
                value = getattr(value, attr)
            return value
        except AttributeError:
            self.get_logger().error(f"Could not get field '{field_path}' from message on topic '{msg}'")
            return None

    def record_data(self):
        with self.data_lock:
            row = {}
            for field in self.fields:
                topic_name = field['topic_name']
                field_name = field['name']
                field_path = field['field_path']
                msg = self.latest_messages.get(topic_name)
                if msg is not None:
                    value = self.get_field_value(msg, field_path)
                    if value is not None:
                        row[field_name] = value
                    else:
                        row[field_name] = ''
                else:
                    row[field_name] = ''
            self.data.append(row)

    def write_csv(self):
        if not self.data:
            self.get_logger().info("No data to write to CSV.")
            return

        field_names = [field['name'] for field in self.fields]
        with open(self.output_csv, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            writer.writeheader()
            for row in self.data:
                writer.writerow(row)
        self.get_logger().info(f"Data written to {self.output_csv}")

def main(args=None):
    rclpy.init(args=args)
    node = DataRecorderNode()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.write_csv()
        node.destroy_node()
        rclpy.shutdown()

if __name__ == '__main__':
    main()
