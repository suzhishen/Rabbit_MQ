import configparser
import os


current_file_path = os.path.abspath(os.path.join(__file__))
current_directory = os.path.dirname(current_file_path)

config = configparser.ConfigParser()

config.read(f'{current_directory}/config.conf', encoding='utf-8')

if 'rabbitmq' in config:
    rabbitmq_config = config['rabbitmq']
else:
    raise Exception("在配置文件 public_tool/rabbit_mq/config/config.conf 中没有找到 rabbitmq 配置.")

