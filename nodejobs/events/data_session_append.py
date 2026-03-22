import os
from nodejobs.dependencies.BaseSession import DataSession as _BaseDataSession


class DataSessionAppend(_BaseDataSession):
    """
    DataSession with explicit append support ('a', 'ab').
    Enforces contained paths and creates parent dirs for write/append modes.
    """

    def open(self, path: str, mode: str = "r"):
        if not self.is_contained_path(path):
            raise Exception(
                f"Not a contained path {path}. Must reside under {self[self.f_root]}"
            )
        assert isinstance(path, str) and len(path) > 0, f"Invalid path: {path!r}"
        assert isinstance(mode, str), "mode must be a string"
        # Support exclusive-create modes 'x' and 'xb' for lockfiles
        allowed = ["r", "rb", "w", "wb", "a", "ab", "x", "xb"]
        if mode not in allowed:
            raise ValueError(f"Unsupported mode: {mode}")

        full_path = os.path.join(self[self.f_root], path)
        # Ensure parent dir exists for any write/append
        if mode in ["w", "wb", "a", "ab", "x", "xb"]:
            directory = os.path.dirname(full_path)
            os.makedirs(directory, exist_ok=True)

        real = os.path.abspath(os.path.normpath(full_path))
        if self[self.f_verbose]:
            kind = "loading" if mode in ["r", "rb"] else "writing"
            print(f"data_session_append open ({kind}) ... {real}")
        return open(real, mode)

    def _abs_path(self, path: str) -> str:
        """Return the absolute filesystem path for a contained relative path."""
        full_path = os.path.join(self[self.f_root], path)
        return os.path.abspath(os.path.normpath(full_path))

    def unlink(self, path: str) -> None:
        """Remove a contained file if it exists (sandboxed)."""
        if not self.is_contained_path(path):
            raise Exception(
                f"Not a contained path {path}. Must reside under {self[self.f_root]}"
            )
        abs_p = self._abs_path(path)
        try:
            os.remove(abs_p)
        except FileNotFoundError:
            return

    def replace(self, src: str, dst: str) -> None:
        """Atomically rename src to dst within the sandbox (os.replace)."""
        if not self.is_contained_path(src) or not self.is_contained_path(dst):
            raise Exception(
                f"Paths must be contained under {self[self.f_root]}: {src} -> {dst}"
            )
        abs_src = self._abs_path(src)
        abs_dst = self._abs_path(dst)
        os.replace(abs_src, abs_dst)
