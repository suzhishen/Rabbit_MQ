# import pika
# import time
# import logging
# import frappe

# import os
# import sys

# parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
# sys.path.append(parent_dir)
# from config.config_parser import rabbitmq_config


# class RabbitMq:
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)

#         self.credentials = pika.PlainCredentials(
#             rabbitmq_config["username"], rabbitmq_config["password"]
#         )

#         self.parameters = pika.ConnectionParameters(
#             host=rabbitmq_config["host"],
#             port=rabbitmq_config["port"],
#             virtual_host=rabbitmq_config["virtual_host"],
#             credentials=self.credentials,
#         )

#         # self.connection = pika.BlockingConnection(self.parameters)
#         # self.channel = self.connection.channel()
#         # self.channel.exchange_declare(exchange='exchange_erpnext', durable=True, exchange_type='direct')  # durable=True 持久化存储

#         self.max_reconnect = 3  # 重连次数
#         self.connection = None
#         self.channel = None

#     def reconnect(self):
#         try:
#             self.connection = pika.BlockingConnection(self.parameters)
#             self.channel = self.connection.channel()
#             self.channel.exchange_declare(
#                 exchange="exchange_erpnext", durable=True, exchange_type="direct"
#             )
#         except pika.exceptions.AMQPConnectionError as e:
#             logging.error(f"连接RabbitMQ失败: {e}")
#             time.sleep(5)
#             logging.error("尝试重新连接到RabbitMQ并创建一个新通道")
#             if self.max_reconnect <= 1:
#                 logging.error(f"连接RabbitMQ失败, 请联系管理员！ {e}")
#                 self.max_reconnect = 3
#                 frappe.log_error()
#                 frappe.throw(f"连接RabbitMQ失败, 请联系管理员！\n {e}")
#             self.max_reconnect -= 1
#             self.reconnect()  # 5s递归重试

#     def producer(self, message):
#         self.reconnect()
#         message = message
#         try:
#             self.channel.basic_publish(
#                 exchange="exchange_erpnext",  # 修改交换机名称
#                 routing_key="erpnext_all",  # 修改路由键
#                 body=message,
#                 properties=pika.BasicProperties(delivery_mode=2),
#             )
#             logging.warning(f"发送信息: {message}")
#         except pika.exceptions.StreamLostError:
#             logging.error("连接丢失，正在尝试重新连接…")
#             self.reconnect()
#             self.producer(message)
#         except Exception as e:
#             logging.error(f"发送消息时出错: {e}")
#             self.max_reconnect = 3
#             frappe.log_error()
#             frappe.throw(f"发送消息时出错, 请联系管理员！\n {e}")


# rabbit_mq = RabbitMq()


import pika
import time
import logging
import frappe

import os
import sys

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
sys.path.append(parent_dir)
from config.config_parser import rabbitmq_config


class RabbitMq:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.credentials = pika.PlainCredentials(
            rabbitmq_config["username"], rabbitmq_config["password"]
        )

        self.parameters = pika.ConnectionParameters(
            host=rabbitmq_config["host"],
            port=rabbitmq_config["port"],
            virtual_host=rabbitmq_config["virtual_host"],
            credentials=self.credentials,
        )

        self.max_reconnect = 3  # 重连次数
        self.connection = None
        self.channel = None

    def reconnect(self, routing_key):
        routing_key = routing_key
        try:
            self.connection = pika.BlockingConnection(self.parameters)
            self.channel = self.connection.channel()
            self.channel.exchange_declare(
                exchange=f"saas_{routing_key}", durable=True, exchange_type="direct"
            )
            # 在 Queues and Streams 消费者显示持久化
            self.channel.queue_declare(queue=f"saas_{routing_key}", durable=True, exclusive=False)
            self.channel.queue_bind(
                exchange=f"saas_{routing_key}",
                queue=f"saas_{routing_key}",
                routing_key=f"saas_{routing_key}",
            )
        except pika.exceptions.AMQPConnectionError as e:
            logging.error(f"连接RabbitMQ失败: {e}")
            time.sleep(5)
            logging.error("尝试重新连接到RabbitMQ并创建一个新通道")
            if self.max_reconnect <= 1:
                logging.error(f"连接RabbitMQ失败, 请联系管理员！ {e}")
                self.max_reconnect = 3
                frappe.log_error()
                frappe.throw(f"连接RabbitMQ失败, 请联系管理员！\n {e}")
            self.max_reconnect -= 1
            self.reconnect(routing_key)  # 5s递归重试

    def producer(self, message, routing_key):
        """
        :param message: 发送的消息内容
        :param routing_key: 消息的 routing_key
        """
        self.reconnect(routing_key)
        try:
            self.channel.basic_publish(
                exchange=f"saas_{routing_key}",  # 修改交换机名称 【消费者主要接收这个】
                routing_key=f"saas_{routing_key}",  # 动态的 routing_key
                body=message,
                properties=pika.BasicProperties(delivery_mode=2, content_type='application/json', content_encoding='utf-8'),
            )
            logging.warning(f"发送信息: {message} 到队列 {routing_key}")
        except pika.exceptions.StreamLostError:
            logging.error("连接丢失，正在尝试重新连接…")
            self.reconnect(routing_key)
            self.producer(message, routing_key)
        except Exception as e:
            logging.error(f"发送消息时出错: {e}")
            self.max_reconnect = 3
            frappe.log_error()
            frappe.throw(f"发送消息时出错, 请联系管理员！\n {e}")


rabbit_mq = RabbitMq()
