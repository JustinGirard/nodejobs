# TestService.py
"""Legacy inheritance example; prefer TestServiceComposable for new servers.

See nodejobs/README.md, "Building command servers", and
nodejobs.dependencies.TestServiceComposable.
"""

import warnings

from nodejobs.dependencies.command_registry_host import CommandRegistryHost

'''
Legacy inheritance-based test fixture. Prefer registering service classes
with CommandRegistryHost as shown in TestServiceComposable.

'''
class TestService(CommandRegistryHost):
    @classmethod
    def run_cli(cls):
        warnings.warn(
            "TestService is a deprecated server template; see nodejobs/README.md "
            "'Building command servers' and "
            "nodejobs.dependencies.TestServiceComposable.",
            DeprecationWarning,
            stacklevel=2,
        )
        return super().run_cli()

    @classmethod
    def get_domain_command_map(cls):
        return {
            "now": {"required_args": ["tz_offset"], "method": cls.now},
            "fixture": {"required_args": ["kind"], "method": cls.fixture},
        }

    @classmethod
    def now(cls, tz_offset: float) -> float:
        return float(tz_offset) + 10

    @classmethod
    def fixture(cls, kind: str):
        fixtures = {
            "int": 3,
            "float": 2.5,
            "string": "abc",
            "bool": True,
            "dict": {"a": 1, "error": "app-level"},
            "list": [1, "x", False],
            "null": None,
            "nested": {"x": [1, {"y": None}]},
        }
        if kind not in fixtures:
            raise Exception(f"unknown_fixture_kind:{kind}")
        return fixtures[kind]

if __name__ == "__main__":
    TestService.run_cli()
# python3 TestService.py now tz_offset=1

# 1 - Get rid of get_command_map
# 2 - Get MCP running
# 3 - update BaseService and Wallet 
