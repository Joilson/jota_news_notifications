import json

import pika

RABBITMQ_URL = 'amqps://fbgzrkop:1K1hfqMAtnYNmhsSGYSjt_v8eOkq8cqu@leopard.lmq.cloudamqp.com/fbgzrkop'


def callback(ch, method, properties, body):
    dados = json.loads(body)
    print(f"Mensagem recebida: {dados}")
    process(dados)


def process(dados):
    print(f"Notificação enviada...")


def consume():
    connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    channel = connection.channel()

    channel.exchange_declare(exchange='news_exchange', exchange_type='fanout')

    args = {
        'x-dead-letter-exchange': 'dead_letter_exchange',
        'x-message-ttl': 10000,
        'x-max-length': 5
    }

    result = channel.queue_declare(queue='notifications', arguments=args)
    channel.exchange_declare(exchange='dead_letter_exchange', exchange_type='fanout')
    channel.queue_declare(queue='notifications_dlx')
    channel.queue_bind(exchange='dead_letter_exchange', queue='notifications_dlx')

    queue_name = result.method.queue
    channel.queue_bind(exchange='news_exchange', queue=queue_name)

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
    channel.start_consuming()

    print(f"Waiting async messages, to exit press CTRL+C")


consume()
