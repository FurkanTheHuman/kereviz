import uuid
import threading
from queue import Empty, Queue


class Status:
    def __init__(self, outer_class_obj, task, task_id):
        self._status = outer_class_obj
        self.task_id = task_id
        self._task = task

    def status(self):
        return self._status.db[self.task_id]["status"]

    def value(self):
        if self.status() == "completed":
            return self._status.db[self.task_id]["value"]
        return None

    def ready(self):
        self._task.join()
        if self.status() == "failed":
            self.raise_error()
        return self.value()

    def get_error(self):
        if self.status() == "failed":
            return self._status.db[self.task_id]["error"]
        return None

    def raise_error(self):
        if self.status() == "failed":
            raise self._status.db[self.task_id]["error"]
        return None


class InMemoryDB(dict):
    def __init__(self, *arg, **kw):
        super(InMemoryDB, self).__init__(*arg, **kw)
        self._topics = dict()

    def add_queue(self, topic, message):
        if topic in self._topics:
            self._topics[topic].put(message)
        else:
            q = Queue()
            self._topics[topic] = q
            self._topics[topic].put(message)

    def get_queue(self, topic, timeout=None, block=True):
        if not topic in self._topics:
            q = Queue()
            self._topics[topic] = q
        try:
            return self._topics[topic].get(block=block, timeout=timeout)
        except Empty:
            return None

class Kereviz:
    def __init__(self):
        self.db = InMemoryDB()

    def tasks(self, function):
        task_id = str(uuid.uuid4())
        self.db[task_id] = {"status": "inactive"}

        def wrapper(*args, **kwargs):
            def manager(*args, **kwargs):
                my_id = task_id
                self.db[my_id] = {"status": "started"}
                try:
                    value = function(*args, **kwargs)
                    self.db[my_id] = {"status": "completed", "value": value}
                except Exception as e:
                    self.db[my_id] = {"status": "failed", "error": e}

            task = threading.Thread(target=manager, daemon=True, args=args, kwargs=kwargs)
            task.start()
            status = Status(self, task, task_id)
            return status

        return wrapper

    def publish(self, topic, message):
        self.db.add_queue(topic, message)

    def subscribe(self, topic, block=True):
        return self.db.get_queue(topic, block=block)
