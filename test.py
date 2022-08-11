import threading
import pika
import time

current_thread = None


class Test:
    internal_lock = threading.Lock()
    should_exit = threading.Event()

    def __init__(self):
        self.parent_thread = threading.current_thread()
        self.parameters = pika.URLParameters("")

        self.connection = pika.BlockingConnection(parameters=self.parameters)
        self.channel = self.connection.channel()
        result = self.channel.queue_declare('', exclusive=True)
        self.callback_queue = result.method.queue
        #self.channel.basic_consume(self.callback_queue, self._on_response, auto_ack=True)
        thread = threading.Thread(target=self._process_data_events)
        thread.setDaemon(True)
        thread.start()

    def _process_data_events(self):
        while not self.should_exit.is_set():
            try:
                while not self.should_exit.is_set():
                    if not self.parent_thread.is_alive():
                        print("Closing")
                        self.connection.close()
                        self.should_exit.set()
                        break
                    with self.internal_lock:
                        try:
                            self.connection.process_data_events()
                        except pika.exceptions.AMQPConnectionError:
                            self.connection = pika.BlockingConnection(parameters=self.parameters)
                            self.channel = self.connection.channel()
                            result = self.channel.queue_declare('', exclusive=True)
                            self.callback_queue = result.method.queue
                            #self.channel.basic_consume(self.callback_queue, self._on_response, auto_ack=True)
                    time.sleep(0.1)
            except:
                traceback.print_exc()
                time.sleep(5)
                self.connection = pika.BlockingConnection(parameters=self.parameters)
                self.channel = self.connection.channel()
                result = self.channel.queue_declare('', exclusive=True)
                self.callback_queue = result.method.queue
                #self.channel.basic_consume(self.callback_queue, self._on_response, auto_ack=True)


def thread1():
    _ = Test()
    time.sleep(5)


if __name__ == "__main__":
    t = threading.Thread(target=thread1)
    t.setDaemon(True)
    t.start()

    while True:
        time.sleep(1)
