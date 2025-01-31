import queue
import time
import threading
from typing import Any

import pytest

import prefect
from prefect import context
from prefect.utilities.collections import DotDict
from prefect.utilities.context import Context


def test_context_sets_variables_inside_context_manager():
    """
    Tests that the context context manager properly sets and removes variables
    """
    with pytest.raises(AttributeError):
        context.a
    with context(a=10):
        assert context.a == 10
    with pytest.raises(AttributeError):
        context.a


def test_setting_context_with_keywords():
    """
    Test accessing context varaibles
    """
    with context(x=1):
        assert context.x == 1


def test_setting_context_with_dict():
    """
    Test accessing context varaibles
    """
    with context(dict(x=1)):
        assert context.x == 1


def test_call_function_inside_context_can_access_context():
    """
    Test calling a function inside a context
    """

    def test_fn() -> Any:
        return context.x

    with pytest.raises(AttributeError):
        test_fn()

    with context(x=1):
        assert test_fn() == 1


def test_nested_contexts_properly_restore_parent_context_when_closed():
    # issue https://gitlab.com/prefect/prefect/issues/16
    with context(a=1):
        assert context.a == 1
        with context(a=2):
            assert context.a == 2
        assert context.a == 1


def test_context_setdefault_method():
    with context():  # run in contextmanager to automatically clear when finished
        assert "a" not in context
        assert context.setdefault("a", 5) == 5
        assert "a" in context
        assert context.setdefault("a", 10) == 5


def test_modify_context_by_assigning_attributes_inside_contextmanager():
    assert "a" not in context
    with context(a=1):
        assert context.a == 1

        context.a = 2
        assert context.a == 2

    assert "a" not in context


def test_context_doesnt_overwrite_all_config_keys():
    old_level = context.config.logging.level
    with context(config=dict(logging=dict(x=1))):
        assert context.config.logging.x == 1
        assert context.config.logging.level == old_level


def test_context_respects_the_dotdict_nature_of_config():
    assert "KEY" not in context.config
    with context(config=dict(KEY=dict(x=1))):
        assert context.config.KEY.x == 1

    assert "KEY" not in context.config


def test_context_respects_the_dict_nature_of_non_config_keys():
    assert "KEY" not in context
    with context(KEY=dict(x=1)):
        with pytest.raises(AttributeError):
            assert context.KEY.x == 1

    assert "KEY" not in context.config


def test_modify_context_by_calling_update_inside_contextmanager():
    assert "a" not in context
    with context(a=1):
        assert context.a == 1

        context.update(a=2)
        assert context.a == 2

    assert "a" not in context


def test_context_loads_values_from_config(monkeypatch):
    subsection = DotDict(password="1234")
    config = DotDict(context=DotDict(subsection=subsection, my_key="my_value"))
    monkeypatch.setattr(prefect.utilities.context, "config", config)
    fresh_context = Context()
    assert "subsection" in fresh_context
    assert fresh_context.my_key == "my_value"
    assert fresh_context.subsection == subsection


def test_context_loads_secrets_from_config(monkeypatch):
    secrets_dict = DotDict(password="1234")
    config = DotDict(context=DotDict(secrets=secrets_dict))
    monkeypatch.setattr(prefect.utilities.context, "config", config)
    fresh_context = Context()
    assert "secrets" in fresh_context
    assert fresh_context.secrets == secrets_dict


def test_context_contextmanager_prioritizes_new_config_keys():
    with prefect.context({"config": {"logging": {"log_to_cloud": "FOO"}}}):
        assert prefect.context.config.logging.log_to_cloud == "FOO"


def test_context_init_prioritizes_new_config_keys():
    ctx = Context(config=dict(logging=dict(log_to_cloud="FOO")))
    assert ctx.config.logging.log_to_cloud == "FOO"


def test_context_init_prioritizes_new_config_keys_when_passed_a_dict():
    old = dict(config=dict(logging=dict(log_to_cloud="FOO")))
    ctx = Context(old)
    assert ctx.config.logging.log_to_cloud == "FOO"


def test_contexts_are_thread_safe():

    result_queue = queue.Queue()

    def get_context_in_thread(q, id, delay=0):
        time.sleep(delay)
        with prefect.context(x=id):
            time.sleep(delay)
            q.put(prefect.context.x)
            time.sleep(delay)

    threads = []
    for i in range(5):
        thread = threading.Thread(
            target=get_context_in_thread, kwargs=dict(q=result_queue, id=i, delay=0.1)
        )
        threads.append(thread)

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    results = set()
    for _ in threads:
        results.add(result_queue.get(block=False))

    assert results == set(range(len(threads)))
