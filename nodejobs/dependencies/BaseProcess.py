"""A minimal convention for managing an operational process."""


class BaseProcess:
    def ensure(self, **kwargs):
        raise NotImplementedError

    def teardown(self, **kwargs):
        raise NotImplementedError

    def status(self, **kwargs):
        raise NotImplementedError
