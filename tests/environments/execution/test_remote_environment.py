import tempfile
from os import path

import cloudpickle
import pytest

import prefect
from prefect.environments import RemoteEnvironment
from prefect.environments.storage import Docker, Memory


def test_create_remote_environment():
    environment = RemoteEnvironment()
    assert environment
    assert environment.executor == prefect.config.engine.executor.default_class
    assert environment.executor_kwargs == {}
    assert environment.labels == set()
    assert environment.logger.name == "prefect.RemoteEnvironment"


def test_create_remote_environment_populated():
    environment = RemoteEnvironment(
        executor="prefect.engine.executors.DaskExecutor",
        executor_kwargs={"address": "test"},
        labels=["foo", "bar", "good"],
    )
    assert environment
    assert environment.executor == "prefect.engine.executors.DaskExecutor"
    assert environment.executor_kwargs == {"address": "test"}
    assert environment.labels == set(["foo", "bar", "good"])


def test_environment_execute():
    with tempfile.TemporaryDirectory() as directory:

        @prefect.task
        def add_to_dict():
            with open(path.join(directory, "output"), "w") as tmp:
                tmp.write("success")

        with open(path.join(directory, "flow_env.prefect"), "w+") as env:
            flow = prefect.Flow("test", tasks=[add_to_dict])
            flow_path = path.join(directory, "flow_env.prefect")
            with open(flow_path, "wb") as f:
                cloudpickle.dump(flow, f)

        environment = RemoteEnvironment()
        storage = Docker(registry_url="test")

        environment.execute(storage, flow_path)

        with open(path.join(directory, "output"), "r") as file:
            assert file.read() == "success"
