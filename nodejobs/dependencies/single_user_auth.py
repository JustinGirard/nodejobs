"""Example single-user token policy for CommandRegistryHost applications.

The application supplies credentials and a private storage path. This module
does not configure a transport or grant access to any command by itself.
"""

import hashlib
import hmac
import json
import os
import secrets
import tempfile
from pathlib import Path


class SingleUserAuth:
    def __init__(self, *, username, password, token_file):
        if not all(isinstance(value, str) and value for value in (username, password, token_file)):
            raise ValueError("admin_auth_configuration_required")
        self.username = username
        self._password = password
        self.token_file = Path(token_file)

    @staticmethod
    def _hash(token):
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    def _stored_hash(self):
        try:
            return json.loads(self.token_file.read_text(encoding="utf-8"))["token_sha256"]
        except FileNotFoundError:
            return None

    def valid(self, token):
        stored = self._stored_hash()
        return (isinstance(token, str) and bool(token) and isinstance(stored, str)
                and hmac.compare_digest(self._hash(token), stored))

    def login(self, username, password):
        if not (isinstance(username, str) and isinstance(password, str)
                and hmac.compare_digest(username, self.username)
                and hmac.compare_digest(password, self._password)):
            return {"status": "error", "error": "auth_required"}
        token = secrets.token_urlsafe(32)
        self.token_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd, temp = tempfile.mkstemp(prefix=".admin-token-", dir=self.token_file.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump({"token_sha256": self._hash(token)}, stream)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp, self.token_file)
        finally:
            if os.path.exists(temp):
                os.unlink(temp)
        return {"status": "ok", "username": self.username, "auth_token": token}

    def logout(self, token):
        if not self.valid(token):
            return {"status": "error", "error": "auth_required"}
        self.token_file.unlink()
        return {"status": "ok"}
