from pathlib import Path

from nodejobs.dependencies.BaseSession import DataSession
from nodejobs.dependencies.field_set import FieldSet

DEBUG = True


def dprint(*args, **kwargs):
    if DEBUG:
        print(*args, **kwargs)


if __name__ == "__main__":
    session = DataSession({"root": str(Path(__file__).parent), DataSession.f_unlocked: True})
    node = FieldSet({"data_session": session, "cache_path": "./field_set_value_cache"})
    output = node.set_data(value="hello")
    dprint(f"PROOF set_data output: {output!r}")
