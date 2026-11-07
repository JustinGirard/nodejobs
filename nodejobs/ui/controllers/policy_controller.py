class PolicyController:
    REFRESH_SEMANTICS = (
        "Initial load and toolbar refresh use the same async path and notification format."
    )
    SELECTION_OWNERSHIP = (
        "SelectionController is the single source of truth for selected row state."
    )
    EDIT_LIFECYCLE = (
        "Edit cancel emits info notice; edit save updates selection and emits info notice."
    )
    ERROR_NOTIFICATION_FORMAT = "ErrorPresenter output is shown directly via NotificationCenter.show_error."

    @staticmethod
    def apply_refresh_policy(runner, fetch_fn, on_success, on_error) -> None:
        runner.run(fetch_fn, on_success, on_error)

    @staticmethod
    def apply_edit_policy(selected_id: str, warn_fn, entity: str) -> bool:
        value = str(selected_id or "").strip()
        if value:
            return True
        warn_fn(f"no selected {entity}")
        return False

    @staticmethod
    def apply_job_edit_status_policy(status: str, warn_fn) -> bool:
        blocked = {"starting", "running", "stopping"}
        value = str(status or "").strip().lower()
        if value in blocked:
            warn_fn(f"cannot edit job while status={value}")
            return False
        return True

    @staticmethod
    def parse_tag_query(query: str):
        raw = str(query or "").strip()
        if not raw:
            return None
        tokens = [part.strip() for part in raw.replace(",", " ").split() if part.strip()]
        if len(tokens) == 0:
            return None
        tags = {}
        for token in tokens:
            if "=" in token:
                key, value = token.split("=", 1)
            elif ":" in token:
                key, value = token.split(":", 1)
            else:
                key, value = token, token
            key_text = str(key or "").strip()
            value_text = str(value or "").strip()
            if key_text and value_text:
                tags[key_text] = value_text
        if len(tags) == 0:
            return None
        return tags

    @staticmethod
    def format_refresh_success(entity: str, count: int, mode: str) -> str:
        return f"loaded {entity}={count} mode={mode}"

    @staticmethod
    def format_edit_saved(entity: str, item_id: str) -> str:
        return f"{entity} edit saved id={item_id}"

    @staticmethod
    def format_edit_cancelled(entity: str) -> str:
        return f"{entity} edit cancelled"

    @staticmethod
    def format_action_success(action: str, entity: str, item_id: str, mode: str | None = None) -> str:
        msg = f"{action} {entity} id={item_id}"
        if mode:
            return f"{msg} mode={mode}"
        return msg

    @staticmethod
    def format_error(ex: Exception, error_presenter) -> str:
        return error_presenter.to_user_message(ex)
