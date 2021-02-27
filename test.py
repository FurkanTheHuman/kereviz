from time import sleep
import pytest
from kereviz import Kereviz


MyError = ValueError


def test_basic():
    app = Kereviz()

    @app.tasks  # note: tasks should not hold any return value. Except when explicitly specified
    def wait_for_it():  # NOTE: if function has a return type, then it should save the result.
        sleep(0.3)
        print("can print")
        return "success"

    handle = wait_for_it()
    assert handle.status() in ("pending", "started")
    sleep(0.5)
    assert handle.status() == "completed"


def test_get_value():
    app = Kereviz()

    @app.tasks
    def wait_for_it():  # NOTE: if function has a return type, then it should save the result.
        sleep(0.3)
        print("can print")
        return "Hello World"

    handle = wait_for_it()
    assert handle.value() is None
    sleep(0.5)
    assert handle.value() == "Hello World"


def test_get_value_no_matter_what():
    app = Kereviz()

    @app.tasks
    def wait_for_it():
        sleep(0.1)
        print("can print")
        return "Hello World"

    handle = wait_for_it()
    assert handle.ready() == "Hello World"


def test_env_capture():
    app = Kereviz()
    value = 0

    @app.tasks
    def wait_for_it():
        nonlocal value
        value = value + 1
        return value

    handle = wait_for_it()
    assert handle.ready() == 1

    handle = wait_for_it()
    assert handle.ready() == 2
    handle = wait_for_it()
    assert handle.ready() == 3


def test_fail():
    app = Kereviz()

    @app.tasks
    def wait_for_it():
        raise NotImplementedError

    handle = wait_for_it()
    assert handle.status() == "failed"
    with pytest.raises(NotImplementedError):
        handle.raise_error()
    error, expected_error = handle.get_error(), NotImplementedError()
    assert type(error) is type(expected_error) and error.args == expected_error.args


def test_fail_on_access():
    app = Kereviz()

    @app.tasks
    def wait_for_it():  # NOTE: if function has a return type, then it should save the result.
        raise NotImplementedError

    handle = wait_for_it()
    with pytest.raises(NotImplementedError):
        handle.ready()


def test_basic_pubsub():
    app = Kereviz()

    @app.tasks
    def first_task():
        app.publish(topic="test", message="HELLO")
        response = app.subscribe(topic="test2")
        return response

    @app.tasks
    def second_task():
        response = app.subscribe(topic="test")
        app.publish(topic="test2", message="HELLO AGAIN")
        return response

    handle1 = first_task()
    handle1 = first_task()
    handle1 = first_task()
    handle1 = first_task()
    handle2 = second_task()
    handle2 = second_task()
    handle2 = second_task()
    handle2 = second_task()
    assert handle1.ready() == "HELLO AGAIN"
    assert handle2.ready() == "HELLO"


def test_timed_pubsub():
    app = Kereviz()

    @app.tasks
    def first_task():
        sleep(0.3)
        app.publish(topic="test", message="HELLO")
        response = app.subscribe(topic="test2")
        return response

    @app.tasks
    def second_task():
        response = app.subscribe(topic="test")
        app.publish(topic="test2", message="HELLO AGAIN")
        sleep(0.1)
        return response

    handle1 = first_task()
    handle2 = second_task()
    assert handle1.ready() == "HELLO AGAIN"
    assert handle2.ready() == "HELLO"

def test_cancel_task():
    # later aligater
    pass


# possible features:
# scheculing for future
# decorator params 
# auto drop fields 




