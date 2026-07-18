import pika
import time
import json
import os
import sys
import threading
import random
import ssl

host = os.environ.get("RABBITMQ_HOST", "rabbitmq-prod.rabbitmq.svc.cluster.local")
port = int(os.environ.get("RABBITMQ_PORT", "5671"))
vhost = os.environ.get("RABBITMQ_VHOST", "vhost_payments")
cert_dir = os.environ.get("RABBITMQ_CERT_DIR", "/etc/rabbitmq-certs")

ca_cert = os.path.join(cert_dir, "ca.crt")
client_cert = os.path.join(cert_dir, "tls.crt")
client_key = os.path.join(cert_dir, "tls.key")

print(f"Connecting to RabbitMQ at {host}:{port}/{vhost} using mTLS certs...")

# Build SSL Context
context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_cert)
context.check_hostname = False
context.verify_mode = ssl.CERT_REQUIRED
context.load_cert_chain(certfile=client_cert, keyfile=client_key)

ssl_options = pika.SSLOptions(context)
credentials = pika.ExternalCredentials()

parameters = pika.ConnectionParameters(
    host=host,
    port=port,
    virtual_host=vhost,
    credentials=credentials,
    ssl_options=ssl_options
)

# Establish connection
connection = None
for i in range(15):
    try:
        connection = pika.BlockingConnection(parameters)
        break
    except Exception as e:
        print(f"Failed to connect: {e}. Retrying in 3s...")
        time.sleep(3)

if not connection:
    print("Could not connect to RabbitMQ. Exiting.")
    sys.exit(1)

channel = connection.channel()
print("Connected successfully!")

def callback(ch, method, properties, body):
    # Extract delivery count from headers
    delivery_count = 0
    if properties.headers and "x-delivery-count" in properties.headers:
        delivery_count = properties.headers["x-delivery-count"]
    
    # Dynamic processing delay (30s congested build-up, 30s fast drain)
    cycle_time = time.time() % 60
    if cycle_time < 30:
        time.sleep(0.04) # 40ms processing time
    else:
        time.sleep(0.001) # 1ms processing time
    
    is_success = random.random() < 0.995
    if is_success:
        ch.basic_ack(delivery_tag=method.delivery_tag)
    else:
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

channel.basic_qos(prefetch_count=100)
channel.basic_consume(queue="stripe_processing_q", on_message_callback=callback)

def run_consumer():
    print("Starting consumer thread...")
    try:
        channel.start_consuming()
    except Exception as e:
        print(f"Consumer error: {e}")

consumer_thread = threading.Thread(target=run_consumer, daemon=True)
consumer_thread.start()

# Publisher loop
time.sleep(2)
pub_conn = None
for i in range(5):
    try:
        pub_conn = pika.BlockingConnection(parameters)
        break
    except Exception:
        time.sleep(2)
        
if not pub_conn:
    print("Failed to start publisher connection. Exiting.")
    sys.exit(1)

pub_chan = pub_conn.channel()
pub_chan.confirm_delivery()

tx_id = 1
while True:
    time.sleep(0.01)
    payload = {
        "tx_id": f"tx-{tx_id}",
        "amount": 250.0,
        "gateway": "stripe",
        "timestamp": time.time()
    }
    body = json.dumps(payload)
    try:
        pub_chan.basic_publish(
            exchange="payment_commands",
            routing_key="stripe",
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )
        print(f" [Publisher] Published Stripe transaction {tx_id} to payment_commands")
        tx_id += 1
    except Exception as e:
        print(f" [Publisher] Failed to send: {e}")
        try:
            pub_conn = pika.BlockingConnection(parameters)
            pub_chan = pub_conn.channel()
            pub_chan.confirm_delivery()
        except Exception:
            pass
