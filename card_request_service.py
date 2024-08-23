import json
import logging
import os
import time
import pika
import random
import requests
from dotenv import load_dotenv
load_dotenv()

# Функция для проверки данных заявки
def validate_request(data):
    required_fields = ['userId', 'app_id', 'cardType', 'cardCategory', 'cardBalance', 'currency', 'firstName', 'lastName', 'cardRequestId']
    for field in required_fields:
        if field not in data:
            print(f"Missing field: {field}")
            return False
    return True

# Функция для отправки сообщений через RabbitMQ
def send_approval(user_id, card_request_id, correlation_id, reply_to, card_data):
    approval_message = {
        'userId': user_id,
        'cardRequestId': card_request_id,
        'status': 'approved',
        'message': 'Your card request has been approved.'
    }

    logging.info(f"Отправка одобрения через RabbitMQ - Сообщение: {approval_message}, Correlation ID: {correlation_id}, Reply To: {reply_to}")
    print(f"Отправка одобрения через RabbitMQ - Сообщение: {approval_message}, Correlation ID: {correlation_id}, Reply To: {reply_to}")

    credentials = pika.PlainCredentials(os.getenv('RABBITMQ_USERNAME'), os.getenv('RABBITMQ_PASSWORD'))
    parameters = pika.ConnectionParameters(os.getenv('RABBITMQ_HOST'), int(os.getenv('RABBITMQ_PORT')), 'gbank', credentials)

    try:
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.basic_publish(
            exchange='gbank_exchange',
            routing_key='card.application.status',
            body=json.dumps(approval_message),
            properties=pika.BasicProperties(
                app_id='1234',
                correlation_id=str(correlation_id),
                reply_to=str(reply_to)
            )
        )
        logging.info("Одобрение успешно отправлено")
        print("Одобрение успешно отправлено")
    except Exception as e:
        logging.error(f"Ошибка при отправке одобрения: {e}")
    finally:
        if 'connection' in locals():
            connection.close()

 # Отправка запроса на генерацию карты
    generate_card_request(card_data)

def generate_card_request(card_data):
    url = 'http://localhost:5001/generate_card'
    payload = {
        'user_id': card_data['userId'],
        'card_request_id': card_data['cardRequestId'],
        'phone': card_data['phone'],
        'card_type': card_data['cardType'],
        'card_category': card_data['cardCategory'],
        'card_balance': card_data['cardBalance'],
        'currency': card_data['currency'],
        'cardholder_firstname': card_data['firstName'],
        'cardholder_lastname': card_data['lastName']
    }

    print(f"Отправка запроса на генерацию карты с данными: {payload}")

    try:
        response = requests.post(url, json=payload)
        if response.status_code == 201:
            print(f"Card generated successfully: {response.json()}")
        else:
            print(f"Failed to generate card: {response.status_code}, {response.text}")
    except Exception as e:
        print(f"Error sending card generation request: {e}")


# Функция для обработки заявок
def callback(ch, method, properties, body):
    message = json.loads(body)
    print(f"Received request: {message}")

    # Проверка данных заявки
    if not validate_request(message):
        print("Invalid request data. Skipping...")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    # Имитация задержки обработки
    time.sleep(random.randint(3, 5))

    # Создание данных карты
    phone_number = message.get('phone')
    if not phone_number:
        print("Phone number is missing. Skipping card generation.")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    card_data = {
        'userId': message.get('userId'),
        'cardRequestId': message.get('cardRequestId'),
        'phone': message.get('phone'),
        'cardType': message['cardType'],
        'cardCategory': message['cardCategory'],
        'cardBalance': message['cardBalance'],
        'currency': message['currency'],
        'firstName': message['firstName'],
        'lastName': message['lastName']
    }

    # Отправка одобрения
    send_approval(message['userId'], message['cardRequestId'], properties.correlation_id, properties.reply_to, card_data)

    # Подтверждение обработки сообщения
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Подключение к RabbitMQ
try:
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
    rabbitmq_port = os.getenv('RABBITMQ_PORT', '5672')
    rabbitmq_username = os.getenv('RABBITMQ_USERNAME', 'guest')
    rabbitmq_password = os.getenv('RABBITMQ_PASSWORD', 'guest')

    # Преобразование порта в int
    rabbitmq_port = int(rabbitmq_port)

    rabbitmq_connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=rabbitmq_host,
        port=rabbitmq_port,
        virtual_host='gbank',
        credentials=pika.PlainCredentials(rabbitmq_username, rabbitmq_password)
    ))
    channel = rabbitmq_connection.channel()
    logging.info("Успешное подключение к RabbitMQ")
    print("Успешное подключение к RabbitMQ")
except Exception as e:
    logging.error(f"Ошибка подключения к RabbitMQ: {e}")
    rabbitmq_connection = None
    raise

# Объявление очереди
channel.queue_declare(queue='card_application_requests', durable=True)

# Подписка на очередь
channel.basic_consume(queue='card_application_requests', on_message_callback=callback)

print('Waiting for messages. To exit press CTRL+C')
try:
    channel.start_consuming()
except KeyboardInterrupt:
    print("Stopping...")
finally:
    if rabbitmq_connection:
        rabbitmq_connection.close()
