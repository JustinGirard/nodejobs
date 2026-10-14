from clients.python.network_cli import ClientCli
from clients.python import ClientService, ClientData, ClientAuth


def test_client_cli_smoke():
    service = ClientService(
        data=ClientData(
            {
                "servers": {
                    "hello": {
                        "port": 8870,
                    }
                }
            }
        ),
        auth=ClientAuth(provider="local"),
    )
    cli = ClientCli(service=service)
    assert cli is not None
