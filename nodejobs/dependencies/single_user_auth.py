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
import time
from pathlib import Path


class SingleUserAuth:
    ttl_seconds = 48 * 60 * 60

    def __init__(self, *, username, password, token_file):
        if not all(isinstance(value, str) and value for value in (username, password, token_file)):
            raise ValueError("admin_auth_configuration_required")
        self.username = username
        self._password = password
        self.token_file = Path(token_file)

    @staticmethod
    def _hash(token):
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    def _tokens(self):
        try:
            data = json.loads(self.token_file.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {}
        if "tokens" in data:
            return data["tokens"]
        # The old single-token file has no timestamp. Its write time is
        # the best available issuance time, preserving an existing login.
        return {
            data["token_sha256"]:
                self.token_file.stat().st_mtime + self.ttl_seconds
        }

    def valid(self, token):
        if not isinstance(token, str) or not token:
            return False
        digest = self._hash(token)
        now = time.time()
        return any(
            hmac.compare_digest(digest, stored) and now < expires
            for stored, expires in self._tokens().items()
        )

    def _save(self, tokens):
        self.token_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd, temp = tempfile.mkstemp(prefix=".admin-token-", dir=self.token_file.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump({"tokens": tokens}, stream)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp, self.token_file)
        finally:
            if os.path.exists(temp):
                os.unlink(temp)

    def login(self, username, password):
        if not (isinstance(username, str) and isinstance(password, str)
                and hmac.compare_digest(username, self.username)
                and hmac.compare_digest(password, self._password)):
            return {"status": "error", "error": "auth_required"}
        token = secrets.token_urlsafe(32)
        now = time.time()
        tokens = {
            digest: expires for digest, expires in self._tokens().items()
            if now < expires
        }
        tokens[self._hash(token)] = now + self.ttl_seconds
        self._save(tokens)
        return {"status": "ok", "username": self.username, "auth_token": token}

    def logout(self, token):
        if not self.valid(token):
            return {"status": "error", "error": "auth_required"}
        tokens = self._tokens()
        del tokens[self._hash(token)]
        self._save(tokens)
        return {"status": "ok"}
