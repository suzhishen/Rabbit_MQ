
import multiprocessing

import pika
import json

from config.config_parser import rabbitmq_config

# 导入工具类
import os
import sys

parent_dir = os.path.abspath(os.path.join(os.path.dirname("../apps/frappe")))
sys.path.append(parent_dir)
import frappe

if __name__ == "__main__":

    def listen_to_queue(queue_name, exchange_name, routing_key):
        """
        为指定队列设置消费者，并处理消息
        """
        credentials = pika.PlainCredentials(rabbitmq_config["username"], rabbitmq_config["password"])
        parameters = pika.ConnectionParameters(
            host=rabbitmq_config["host"],
            port=rabbitmq_config["port"],
            virtual_host=rabbitmq_config["virtual_host"],
            credentials=credentials,
        )

        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        # 声明队列和交换机
        result = channel.queue_declare(queue=queue_name, durable=True, exclusive=False)
        channel.exchange_declare(
            exchange=exchange_name, durable=True, exchange_type="direct"
        )
        channel.queue_bind(
            exchange=exchange_name,
            queue=result.method.queue,
            routing_key=routing_key,
        )

        # 定义回调函数
        def callback(ch, method, properties, body):
            ch.basic_ack(delivery_tag=method.delivery_tag)
            try:
                print(f"收到的信息:{json.loads(body.decode())}")
            except Exception as e:
                print(f"收到的信息: {body.decode()}")

        channel.basic_consume(result.method.queue, callback, auto_ack=False)

        print(f"开始监听队列: {queue_name}，按 CTRL+C 停止")
        channel.start_consuming()


    def start_listening():
        """
        启动多个进程监听不同的队列
        """
        # 假设有多个队列和交换机需要监听
        queues = [
            ("saas_purchase_order.submit", "saas_purchase_order.submit", "saas_purchase_order.submit"),
            ("saas_purchase_order.cancel", "saas_purchase_order.cancel", "saas_purchase_order.cancel"),
            ("saas_stock_entry.submit", "saas_stock_entry.submit", "saas_stock_entry.submit"),
            ("saas_stock_entry.cancel", "saas_stock_entry.cancel", "saas_stock_entry.cancel"),
            ("saas_delivery_note.submit", "saas_delivery_note.submit", "saas_delivery_note.submit"),
            ("saas_delivery_note.cancel", "saas_delivery_note.cancel", "saas_delivery_note.cancel"),
            ("saas_purchase_receipt.submit", "saas_purchase_receipt.submit", "saas_purchase_receipt.submit"),
            ("saas_purchase_receipt.cancel", "saas_purchase_receipt.cancel", "saas_purchase_receipt.cancel"),
        ]

        # 使用多进程监听多个队列
        processes = []
        for queue_name, exchange_name, routing_key in queues:
            p = multiprocessing.Process(target=listen_to_queue, args=(queue_name, exchange_name, routing_key))
            processes.append(p)
            p.start()

        # 等待所有进程结束
        for p in processes:
            p.join()


if __name__ == "__main__":
    start_listening()
