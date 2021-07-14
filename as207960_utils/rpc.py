import pika
import threading
import time
import uuid
from django.conf import settings


class TimeoutError(Exception):
    pass


class InnerRpcClient:
    internal_lock = threading.Lock()
    queue = {}

    def __init__(self):
        self.parameters = pika.URLParameters(settings.RABBITMQ_RPC_URL)
        self.connection = pika.BlockingConnection(parameters=self.parameters)
        self.channel = self.connection.channel()
        result = self.channel.queue_declare('', exclusive=True)
        self.callback_queue = result.method.queue
        thread = threading.Thread(target=self._process_data_events)
        thread.setDaemon(True)
        thread.start()

    def _process_data_events(self):
        while True:
            with self.internal_lock:
                self.channel.basic_consume(self.callback_queue, self._on_response, auto_ack=True)
            try:
                self.channel.start_consuming()
            except pika.exceptions.ChannelClosed:
                self.connection = pika.BlockingConnection(parameters=self.parameters)
                self.channel = self.connection.channel()
                result = self.channel.queue_declare('', exclusive=True)
                self.callback_queue = result.method.queue

    def _on_response(self, ch, method, props, body):
        self.queue[props.correlation_id] = body

    def send_request(self, rpc_queue, payload):
        corr_id = str(uuid.uuid4())
        with self.internal_lock:
            self.queue[corr_id] = None
            self.channel.basic_publish(
                exchange='', routing_key=rpc_queue, properties=pika.BasicProperties(
                    reply_to=self.callback_queue,
                    correlation_id=corr_id,
                ), body=payload
            )
        return corr_id

    def call(self, rpc_queue, payload, timeout=0):
        corr_id = self.send_request(rpc_queue, payload)

        if timeout:
            end = time.time() + timeout
        else:
            end = None

        while self.queue[corr_id] is None:
            if end is not None and time.time() > end:
                raise TimeoutError()
            
            time.sleep(0.1)

        val = self.queue[corr_id]
        del self.queue[corr_id]
        return val


class RpcClient:
    def __init__(self):
        self.storage = threading.local()

    def __get_client(self):
        existing_client = getattr(self.storage, 'client', None)
        if existing_client:
            return existing_client
        else:
            new_client = InnerRpcClient()
            self.storage.client = new_client
            return new_client

    def call(self, *args, **kwargs):
        client = self.__get_client()
        return client.call(*args, **kwargs)
