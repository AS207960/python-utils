import json
import pika


def on_response(ch, method, props, body):
    data = json.loads(body)
    print(method.routing_key, data)


def main():
    parameters = pika.URLParameters("")
    connection = pika.BlockingConnection(parameters=parameters)
    channel = connection.channel()
    queue_result = channel.queue_declare('', exclusive=True)
    callback_queue = queue_result.method.queue

    channel.basic_consume(callback_queue, on_response, auto_ack=True)

    channel.queue_bind(
        exchange='keycloak',
        queue=callback_queue,
        routing_key="KK.EVENT.CLIENT.master.SUCCESS.account.#"
    )

    channel.start_consuming()


if __name__ == "__main__":
    main()
