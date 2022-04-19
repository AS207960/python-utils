import pika
import threading
import time
import uuid
import traceback
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
        self.channel.basic_consume(self.callback_queue, self._on_response, auto_ack=True)
        thread = threading.Thread(target=self._process_data_events)
        thread.setDaemon(True)
        thread.start()

    def _process_data_events(self):
        while True:
            try:
                while True:
                    with self.internal_lock:
                        try:
                            self.connection.process_data_events()
                        except pika.exceptions.AMQPConnectionError:
                            self.connection = pika.BlockingConnection(parameters=self.parameters)
                            self.channel = self.connection.channel()
                            result = self.channel.queue_declare('', exclusive=True)
                            self.callback_queue = result.method.queue
                            self.channel.basic_consume(self.callback_queue, self._on_response, auto_ack=True)
                    time.sleep(0.1)
            except:
                traceback.print_exc()
                time.sleep(5)
                self.connection = pika.BlockingConnection(parameters=self.parameters)
                self.channel = self.connection.channel()
                result = self.channel.queue_declare('', exclusive=True)
                self.callback_queue = result.method.queue
                self.channel.basic_consume(self.callback_queue, self._on_response, auto_ack=True)

    def _on_response(self, ch, method, props, body):
        self.queue[props.correlation_id] = body

    def send_request(self, rpc_queue, payload, timeout):
        corr_id = str(uuid.uuid4())
        with self.internal_lock:
            self.queue[corr_id] = None
            try:
                self.channel.basic_publish(
                    exchange='', routing_key=rpc_queue, properties=pika.BasicProperties(
                        reply_to=self.callback_queue,
                        correlation_id=corr_id,
                        expiration=str(int(timeout * 1000)) if timeout else None
                    ), body=payload
                )
            except (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelWrongStateError):
                self.connection = pika.BlockingConnection(parameters=self.parameters)
                self.channel = self.connection.channel()
                result = self.channel.queue_declare('', exclusive=True)
                self.callback_queue = result.method.queue
                self.channel.basic_consume(self.callback_queue, self._on_response, auto_ack=True)
                self.channel.basic_publish(
                    exchange='', routing_key=rpc_queue, properties=pika.BasicProperties(
                        reply_to=self.callback_queue,
                        correlation_id=corr_id,
                        expiration=str(int(timeout * 1000)) if timeout else None
                    ), body=payload
                )

        return corr_id

    def call(self, rpc_queue, payload, timeout=0):
        corr_id = self.send_request(rpc_queue, payload, timeout)

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
