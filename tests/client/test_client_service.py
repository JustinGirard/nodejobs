from clients.python import ClientService, ClientData, ClientAuth


def test_client_service_smoke():
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
    assert service is not None
