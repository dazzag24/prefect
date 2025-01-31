"""
A Secret is a serializable object used to represent a secret key & value.

The value of the `Secret` is not set upon initialization and instead is set
either in `prefect.context` or on the server, with behavior dependent on the value
of the `use_local_secrets` flag in your Prefect configuration file.

To set a Secret in Prefect Cloud, you can use `prefect.Client.set_secret`, or set it directly via GraphQL:

```graphql
mutation {
  setSecret(input: { name: "KEY", value: "VALUE" }) {
    success
  }
}
```

To set a _local_ Secret, either place the value in your user configuration file (located at `~/.prefect/config.toml`):

```
[context.secrets]
MY_KEY = "MY_VALUE"
```

or directly in context:

```python
import prefect

prefect.context.setdefault("secrets", {}) # to make sure context has a secrets attribute
prefect.context.secrets["MY_KEY"] = "MY_VALUE"
```

::: tip
When settings secrets via `.toml` config files, you can use the [TOML Keys](https://github.com/toml-lang/toml#keys) docs for data structure specifications. Running `prefect` commands with invalid `.toml` config files will lead to tracebacks that contain references to: `..../toml/decoder.py`.
:::

"""

import json
import os
from typing import Any, Optional

import prefect
from prefect.client.client import Client
from prefect.utilities.collections import as_nested_dict


class Secret:
    """
    A Secret is a serializable object used to represent a secret key & value.

    Args:
        - name (str): The name of the secret

    The value of the `Secret` is not set upon initialization and instead is set
    either in `prefect.context` or on the server, with behavior dependent on the value
    of the `use_local_secrets` flag in your Prefect configuration file.

    If using local secrets, `Secret.get()` will attempt to call `json.loads` on the
    value pulled from context.  For this reason it is recommended to store local secrets as
    JSON documents to avoid ambiguous behavior (e.g., `"42"` being parsed as `42`).
    """

    def __init__(self, name: str):
        self.name = name

    def get(self) -> Optional[Any]:
        """
        Retrieve the secret value.  If not found, returns `None`.

        If using local secrets, `Secret.get()` will attempt to call `json.loads` on the
        value pulled from context.  For this reason it is recommended to store local secrets as
        JSON documents to avoid ambiguous behavior.

        Returns:
            - Any: the value of the secret; if not found, raises an error

        Raises:
            - ValueError: if `.get()` is called within a Flow building context, or if `use_local_secrets=True`
                and your Secret doesn't exist
            - ClientError: if `use_local_secrets=False` and the Client fails to retrieve your secret
        """
        if isinstance(prefect.context.get("flow"), prefect.core.flow.Flow):
            raise ValueError(
                "Secrets should only be retrieved during a Flow run, not while building a Flow."
            )

        if prefect.context.config.cloud.use_local_secrets is True:
            secrets = prefect.context.get("secrets", {})
            try:
                value = secrets[self.name]
            except KeyError:
                raise ValueError(
                    'Local Secret "{}" was not found.'.format(self.name)
                ) from None
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value
        else:
            client = Client()
            result = client.graphql(
                """
                query($name: String!) {
                    secretValue(name: $name)
                }
                """,
                variables=dict(name=self.name),
            )  # type: Any
            return as_nested_dict(result.data.secretValue, dict)
