import pika
import time
import logging

import os
import sys
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
sys.path.append(parent_dir)
from config.config_parser import rabbitmq_config

class RabbitMq():
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.credentials = pika.PlainCredentials(rabbitmq_config['username'], rabbitmq_config['password'])

        self.parameters = pika.ConnectionParameters(
            host=rabbitmq_config['host'],
            port=rabbitmq_config['port'],
            virtual_host=rabbitmq_config['virtual_host'],
            credentials=self.credentials
        )

        # self.connection = pika.BlockingConnection(self.parameters)
        # self.channel = self.connection.channel()
        # self.channel.exchange_declare(exchange='exchange_my_queue1', durable=True, exchange_type='direct')  # durable=True 持久化存储
        
        self.max_reconnect = 3  # 重连次数
        self.connection = None
        self.channel = None


    def reconnect(self):
        try:
            self.connection = pika.BlockingConnection(self.parameters)
            self.channel = self.connection.channel()
            self.channel.exchange_declare(
                exchange='exchange_my_queue1', 
                durable=True, 
                exchange_type='direct'
                )
        except pika.exceptions.AMQPConnectionError as e:
            print(f"连接RabbitMQ失败: {e}")
            time.sleep(5)
            print("尝试重新连接到RabbitMQ并创建一个新通道.")
            if self.max_reconnect <= 1:
                logging.warning(f"连接RabbitMQ失败, 请联系管理员！{e}")
                logging.error(f"连接RabbitMQ失败, 请联系管理员！{e}")
                self.max_reconnect = 3
            self.max_reconnect -= 1
            self.reconnect()  # 5s递归重试

    def producer(self, message):
        self.reconnect()
        message = message
        try:
            self.channel.basic_publish(
                exchange='exchange_my_queue1',
                routing_key='my_queue1',
                body=message,
                properties=pika.BasicProperties(delivery_mode=2)
            )
            print(f"Sent message: {message}")
        except pika.exceptions.StreamLostError:
            print("连接丢失，正在尝试重新连接…")
            self.reconnect()
            self.producer(message)
        except Exception as e:
            print(f"发送消息时出错: {e}")
            if self.connection is not None:
                self.connection.close()
            self.reconnect()

rabbit_mq = RabbitMq()
rabbit_mq.producer(message="我发送了一条消息")

