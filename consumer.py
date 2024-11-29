# import pika
# import json

# from config.config_parser import rabbitmq_config

# # 导入工具类
# import os
# import sys

# parent_dir = os.path.abspath(os.path.join(os.path.dirname("../apps/frappe")))
# sys.path.append(parent_dir)
# import frappe

# if __name__ == "__main__":
#     # """
#     # 消费者, 非持久化数据
#     # """
#     # credentials = pika.PlainCredentials('admin', 'admin')

#     # parameters = pika.ConnectionParameters(
#     #     host='zxy.lsun.net',
#     #     port=611,
#     #     virtual_host='my_vhost',
#     #     credentials=credentials
#     # )

#     # connection = pika.BlockingConnection(parameters)
#     # channel = connection.channel()

#     # channel.queue_declare(queue='my_queue')

#     # def callback(ch, method, properties, body):
#     #     ch.basic_ack(delivery_tag=method.delivery_tag)
#     #     print(f"Received message: {body.decode()}")

#     # channel.basic_consume(queue='my_queue', on_message_callback=callback, auto_ack=False)

#     # print('Waiting for messages. To exit press CTRL+C')
#     # channel.start_consuming()

#     """
#     消费者, 持久化数据
#     """
#     credentials = pika.PlainCredentials(
#         rabbitmq_config["username"], rabbitmq_config["password"]
#     )

#     parameters = pika.ConnectionParameters(
#         host=rabbitmq_config["host"],
#         port=rabbitmq_config["port"],
#         virtual_host=rabbitmq_config["virtual_host"],
#         credentials=credentials,
#     )

#     connection = pika.BlockingConnection(parameters)
#     channel = connection.channel()

#     result = channel.queue_declare(queue="erpnext_all", durable=True, exclusive=False)
#     channel.exchange_declare(
#         exchange="exchange_erpnext", durable=True, exchange_type="direct"
#     )
#     channel.queue_bind(
#         exchange="exchange_erpnext",
#         queue=result.method.queue,
#         routing_key="erpnext_all",
#     )

#     def callback(ch, method, properties, body):
#         # print(body.decode())
#         # return
#         # raise ValueError("获取值{}".format(body.decode()))
#         # 消费者收到成功消费的消息后才从消息队列删除
#         # article1 = frappe.get_all("Article", filters={})
#         # print(article1)

#         ch.basic_ack(delivery_tag=method.delivery_tag)
#         try:
#             print(f"收到的信息:{json.loads(body.decode())}")
#         except:
#             print(f"收到的信息: {body.decode()}")

#     channel.basic_consume(result.method.queue, callback, auto_ack=False)

#     print("等待消息，退出按 CTRL+C")
#     channel.start_consuming()






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
    # """
    # 消费者, 非持久化数据
    # """
    # credentials = pika.PlainCredentials('admin', 'admin')

    # parameters = pika.ConnectionParameters(
    #     host='zxy.lsun.net',
    #     port=611,
    #     virtual_host='my_vhost',
    #     credentials=credentials
    # )

    # connection = pika.BlockingConnection(parameters)
    # channel = connection.channel()

    # channel.queue_declare(queue='my_queue')

    # def callback(ch, method, properties, body):
    #     ch.basic_ack(delivery_tag=method.delivery_tag)
    #     print(f"Received message: {body.decode()}")

    # channel.basic_consume(queue='my_queue', on_message_callback=callback, auto_ack=False)

    # print('Waiting for messages. To exit press CTRL+C')
    # channel.start_consuming()

    """
    消费者, 持久化数据
    """
    credentials = pika.PlainCredentials(
        rabbitmq_config["username"], rabbitmq_config["password"]
    )

    parameters = pika.ConnectionParameters(
        host=rabbitmq_config["host"],
        port=rabbitmq_config["port"],
        virtual_host=rabbitmq_config["virtual_host"],
        credentials=credentials,
    )

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    result = channel.queue_declare(queue="purchase_order.submit", durable=True, exclusive=False)
    channel.exchange_declare(
        exchange="saas_purchase_order.submit", durable=True, exchange_type="direct"
    )
    channel.queue_bind(
        exchange="saas_purchase_order.submit",
        queue=result.method.queue,
        routing_key="purchase_order.submit",
    )

    def callback(ch, method, properties, body):
        # print(body.decode())
        # return
        # raise ValueError("获取值{}".format(body.decode()))
        # 消费者收到成功消费的消息后才从消息队列删除
        # article1 = frappe.get_all("Article", filters={})
        # print(article1)

        ch.basic_ack(delivery_tag=method.delivery_tag)
        try:
            print(f"收到的信息:{json.loads(body.decode())}")
        except:
            print(f"收到的信息: {body.decode()}")

    channel.basic_consume(result.method.queue, callback, auto_ack=False)

    print("等待消息，退出按 CTRL+C")
    channel.start_consuming()

