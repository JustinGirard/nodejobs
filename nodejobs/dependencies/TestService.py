# TestService.py
import datetime
from decelium_wallet.commands.BaseData import BaseData
from decelium_wallet.commands.BaseService import BaseService

'''
The smallest way to create a CLI service and registry

'''
class TestService(BaseService):
    @classmethod
    def get_command_map(cls):
        return {
            "now": {"required_args": ["tz_offset"], "method": cls.now},
        }

    @classmethod
    def now(cls, tz_offset: float) -> float:
        return float(tz_offset) + 10

if __name__ == "__main__":
    TestService.run_cli()
# python3 TestService.py now tz_offset=1

# 1 - Get rid of get_command_map
# 2 - Get MCP running
# 3 - update BaseService and Wallet 
