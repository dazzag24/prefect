import cloudpickle
import os
import pendulum
import subprocess
import tempfile

import pytest

from prefect import context, Flow
from prefect.engine import signals
from prefect.tasks.templates import JinjaTemplateTask, StringFormatterTask
from prefect.utilities.debug import raise_on_exception


def test_string_formatter_simply_formats():
    task = StringFormatterTask(template="{name} is from {place}")
    with Flow(name="test") as f:
        ans = task(name="Ford", place="Betelgeuse")
    res = f.run()
    assert res.is_successful()
    assert res.result[ans].result == "Ford is from Betelgeuse"


def test_string_formatter_can_be_provided_template_at_runtime():
    task = StringFormatterTask()
    with Flow(name="test") as f:
        ans = task(template="{name} is from {place}", name="Ford", place="Betelgeuse")
    res = f.run()
    assert res.is_successful()
    assert res.result[ans].result == "Ford is from Betelgeuse"


def test_string_formatter_formats_from_context():
    task = StringFormatterTask(template="I am {task_name}", name="foo")
    f = Flow(name="test", tasks=[task])
    res = f.run()
    assert res.is_successful()
    assert res.result[task].result == "I am foo"


def test_string_formatter_fails_in_expected_ways():
    t1 = StringFormatterTask(template="{name} is from {place}")
    t2 = StringFormatterTask(template="{0} is from {1}")
    f = Flow(name="test", tasks=[t1, t2])
    res = f.run()

    assert res.is_failed()
    assert isinstance(res.result[t1].result, KeyError)
    assert isinstance(res.result[t2].result, IndexError)


def test_jinja_template_simply_formats():
    task = JinjaTemplateTask(template="{{ name }} is from {{ place }}")
    with Flow(name="test") as f:
        ans = task(name="Ford", place="Betelgeuse")
    res = f.run()
    assert res.is_successful()
    assert res.result[ans].result == "Ford is from Betelgeuse"


def test_jinja_template_can_be_provided_template_at_runtime():
    task = JinjaTemplateTask()
    with Flow(name="test") as f:
        ans = task(
            template="{{ name }} is from {{ place }}", name="Ford", place="Betelgeuse"
        )
    res = f.run()
    assert res.is_successful()
    assert res.result[ans].result == "Ford is from Betelgeuse"


def test_jinja_template_formats_from_context():
    task = JinjaTemplateTask(template="I am {{ task_name }}", name="foo")
    f = Flow(name="test", tasks=[task])
    res = f.run()
    assert res.is_successful()
    assert res.result[task].result == "I am foo"


def test_jinja_template_partially_formats():
    task = JinjaTemplateTask(template="{{ name }} is from {{ place }}")
    with Flow(name="test") as f:
        ans = task(name="Ford")
    res = f.run()
    assert res.is_successful()
    assert res.result[ans].result == "Ford is from "


def test_jinja_template_can_execute_python_code():
    date = pendulum.parse("1986-09-20")
    task = JinjaTemplateTask(template='{{ date.strftime("%Y-%d") }} is a date.')
    f = Flow(name="test", tasks=[task])
    res = f.run(context={"date": date})

    assert res.is_successful()
    assert res.result[task].result == "1986-20 is a date."


def test_jinja_task_is_pickleable():
    task = JinjaTemplateTask(template="string")
    new = cloudpickle.loads(cloudpickle.dumps(task))

    assert isinstance(new, JinjaTemplateTask)
    assert new.template == "string"
