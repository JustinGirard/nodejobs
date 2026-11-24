"""Create a local node, invoke set_data through Fragmint, and print its output."""

import os
from pathlib import Path

from fragmint import Fragmint
from fragmint.client.models import AccountCredentials, ConnectionSpec

DEBUG = True


def dprint(*args, **kwargs):
    if DEBUG:
        print(*args, **kwargs)


if __name__ == "__main__":
    root = Path(__file__).parent / "fragmint_local_field_set_value_data"
    host = root / "host"
    private_root = root / "client"
    host.mkdir(parents=True, exist_ok=True)
    private_root.mkdir(parents=True, exist_ok=True)

    connection = Fragmint.connect(ConnectionSpec({
        ConnectionSpec.host: str(host),
        ConnectionSpec.account: AccountCredentials({
            AccountCredentials.email: os.environ["FRAGMINT_EMAIL"],
            AccountCredentials.token: os.environ["FRAGMINT_TOKEN"],
        }),
        ConnectionSpec.private_root: str(private_root),
    }))
    created = connection.run(__command=["create_node"], payload={
        "node": {"type": "FieldSet", "name": "Value", "fields": {}},
    })
    output = connection.FieldSet.set_data(
        target={"id": created.id},
        value="hello",
    )
    assert output["value"] == "hello", output
    dprint(f"PROOF set_data output: {output!r}")
