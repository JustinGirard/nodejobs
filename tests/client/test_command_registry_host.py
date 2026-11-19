from nodejobs.dependencies.TestService import TestService


def test_command_registry_host_map_shape():
    cmap = TestService.get_command_map()
    assert "launch" in cmap
    assert "serve" in cmap
    assert "list_commands" in cmap
    assert "now" in cmap
    assert "fixture" in cmap


def test_command_registry_host_list_commands_metadata():
    meta = TestService.list_commands()
    assert meta["launch"]["infra"] is True
    assert meta["serve"]["infra"] is True
    assert meta["now"]["infra"] is False
    assert meta["fixture"]["infra"] is False
