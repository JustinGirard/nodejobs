class ErrorPresenter:
    @staticmethod
    def to_user_message(ex: Exception) -> str:
        name = ex.__class__.__name__
        text = str(ex) or "unknown error"
        return f"{name}: {text}"
