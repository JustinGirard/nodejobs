import os, shutil
import json
import re
import io
from nodejobs.dependencies.BaseData import BaseData

class BaseSession(BaseData):
    required_methods = ["exists", "delete", "open", "listdir","isfile","isfolder"]
    f_root = "root"
    def get_keys(self):
        required = {self.f_root: str}
        optional = {}
        return required, optional

    def __init__(self, in_dict, trim=False):
        #self._check_required_methods(in_dict)
        super().__init__(in_dict, trim=trim)

    def _check_required_methods(self,in_dict):
        available_methods = dir(self)
        for method_name in self.required_methods:
            if method_name not in available_methods:
                raise NotImplementedError(
                    f"The method '{method_name}' must be implemented in '{self.__class__.__name__}: {in_dict}"
                )
class MemorySession(BaseSession):
    """
    A simplified in-memory Session class to provide a basic IO interface for testing.
    Stores file contents in an in-memory dictionary.
    """
    f_storage = 'storage'
    f_root = 'root'  # To specify root for specification reasons, though ignored
    class StringStreamWrapper:
        """A wrapper to manage a stream and store its content as a string."""
        
        def __init__(self, storage, path, mode='r'):
            self.storage = storage
            self.path = path
            self.buffer = io.BytesIO() if 'b' in mode else io.StringIO()

        def __enter__(self):
            return self.buffer

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.buffer.seek(0)
            if isinstance(self.buffer, io.BytesIO):
                # Convert bytes to string before storing
                self.storage[self.path] = self.buffer.read().decode()
            else:
                self.storage[self.path] = self.buffer.getvalue()

    def __init__(self, in_dict, trim=False):
        super().__init__(in_dict, trim=trim)
    
    def do_pre_process(self, in_dict):
        if self.f_storage not in in_dict:
            in_dict[self.f_storage] = {}
        return super().do_pre_process(in_dict)
        
    def get_keys(self):
        required = {self.f_root: str,self.f_storage:dict}
        optional = {}
        return required, optional

    def do_validation(self, key, value):
        if key == self.f_storage:
            if  not all(isinstance(k, str) and isinstance(v, str) for k, v in value.items()):
                raise ValueError(f"All keys and values in storage must be strings {value}")
        return value, ""

    def exists(self, path: str) -> bool:
        assert isinstance(path, str) and path, "Must supply a non-empty path"
        return path in self[self.f_storage]

    def delete(self, path: str) -> bool:
        assert isinstance(path, str) and path, "Must supply a non-empty path"
        if path in self[self.f_storage]:
            del self[self.f_storage][path]
            return True
        return False

    def open(self, path: str, mode: str = 'r'):
        assert isinstance(path, str) and path, "Must supply a non-empty path"
        assert isinstance(mode, str) and mode in ['r', 'rb', 'w', 'wb']

        storage = self[self.f_storage]
        if mode in ['r', 'rb']:
            if path not in storage:
                raise FileNotFoundError(f"No such file or directory: '{path}'")

            # Convert stored content to appropriate stream type
            content = storage[path]
            if mode == 'r':
                stream = io.StringIO(content) if isinstance(content, str) else io.StringIO(content.decode())
            else:  # 'rb'
                stream = io.BytesIO(content.encode()) if isinstance(content, str) else io.BytesIO(content)
            return stream

        if mode in ['w', 'wb']:
            return  self.StringStreamWrapper(storage, path, mode)

    def listdir(self, dir_path: str):
        """Lists the contents of the directory at the given path within the in-memory storage."""
        assert isinstance(dir_path, str) and dir_path, "Must supply a non-empty path"
        
        # Normalize the directory path without leading './'
        norm_dir_path = os.path.normpath(dir_path).lstrip('./')
        norm_dir_path.lstrip('/')
        # Ensure the path ends with a slash for accurate directory matching
        if norm_dir_path and not norm_dir_path.endswith('/'):
            norm_dir_path += '/'

        # To collect directory contents
        listed_items = set()
        for key in self[self.f_storage].keys():
            # Normalize each stored path without leading './'
            norm_key = os.path.normpath(key).lstrip('/')

            # Check if the key starts with the directory path
            if norm_key.startswith(norm_dir_path):
                # Extract the portion of the path after the directory prefix
                rest_of_path = norm_key[len(norm_dir_path):]
                # Split on '/', only the first item after the directory prefix is considered
                parts = rest_of_path.split('/', 1)
                if parts[0]:  # Ensure it is non-empty
                    listed_items.add(parts[0])
        
        return list(listed_items)
          
class DataSession(BaseSession):
    """
    A simplified Session class to provide a basic IO interface for testing with the file system.
    Robust -- will not allow out-of-session file ops. (however it can easily be hacked -- not a security barrier)
    """
    f_root = 'root'
    f_verbose = 'verbose'
    f_unlocked = 'unlocked'
    #f_storage = '__storage'
    
    def list_recursively(self): #TODO Refactor this out
        """A generator function to yield files in the directory recursively."""
        session = self
        queue = ["./"]
        while queue:
            current_path = queue.pop()
            print(current_path)
            for filename in session.listdir(current_path):
                full_path = f"{current_path}/{filename}"
                if session.isfile(full_path):
                    yield current_path, filename
                elif session.isfolder(full_path):
                    queue.append(full_path)
    def from_session_path(self, path):
        doc_root = self[DataSession.f_root]
        return os.path.relpath(path, doc_root)

    def to_local_path(self, path):
        # Enforce containment at the conversion boundary.
        if not self.is_contained_path(path):
            raise Exception("Not a contained path")
        doc_root = self[DataSession.f_root]
        return os.path.abspath(os.path.join(doc_root, path))

    def stat(self, path: str):
        return os.stat(self.to_local_path(path))


    def __init__(self, in_dict, trim=False):
        if self.f_verbose not in in_dict:
            in_dict[self.f_verbose] = False
        if self.f_unlocked not in in_dict:
            in_dict[self.f_unlocked] = False

        super().__init__(in_dict, trim=trim) 

    # def is_contained_path(self, path: str) -> bool:
    #     if (self[self.f_unlocked] == True):
    #         return True
    #     norm = os.path.normpath(path)
    #     return not norm.startswith('..') and  not norm.startswith('/')
    def is_contained_path(self, path: str) -> bool:
        if self[self.f_unlocked] == True:
            return True
        norm = os.path.normpath(path)
        if os.path.isabs(norm):
            root = os.path.abspath(self[self.f_root])
            return os.path.commonpath([root, norm]) == root
        return not norm.startswith('..') and not norm.startswith('/')
    

    def get_keys(self):
        required = {self.f_root: str}
        optional = {self.f_unlocked:bool, self.f_verbose:bool}
        return required, optional

    @staticmethod
    def path_string_replace(replacements, target: str) -> str:
        """
        Boundary-safe path/string replacement utility.
        - replacements: list of (src, dst) pairs
        - longest src wins
        - slash-insensitive matching (/ and \\)
        - case-insensitive matching
        - preserves trailing separator when consumed by match
        """
        if not isinstance(target, str) or not target:
            return target
        if not isinstance(replacements, (list, tuple)) or len(replacements) == 0:
            return target

        normalized = []
        for item in replacements:
            if not isinstance(item, (list, tuple)) or len(item) != 2:
                continue
            src_raw, dst_raw = item[0], item[1]
            if not isinstance(src_raw, str) or not src_raw.strip():
                continue
            if not isinstance(dst_raw, str):
                continue

            src_norm = src_raw.strip().replace("\\", "/").rstrip("/\\")
            if not src_norm:
                continue
            dst_norm = dst_raw.strip().replace("\\", "/").rstrip("/\\")
            normalized.append((src_norm, dst_norm))

        if len(normalized) == 0:
            return target

        normalized.sort(key=lambda x: len(x[0]), reverse=True)

        groups = []
        replacement_by_group = {}
        for idx, (src_norm, dst_norm) in enumerate(normalized):
            group_name = f"r{idx}"
            src_pat = re.escape(src_norm).replace("/", r"[\\/]")
            groups.append(f"(?P<{group_name}>{src_pat})")
            replacement_by_group[group_name] = dst_norm

        group_union = "|".join(groups)
        boundary_class = r"[\s\"'<>,\[\]\(\){};:]"
        pattern_src = r"(?:" + group_union + r")(?:[\\/]|(?=$|" + boundary_class + r"))"
        pattern = re.compile(pattern_src, flags=re.IGNORECASE)

        def _sub(match: re.Match) -> str:
            replacement = replacement_by_group.get(match.lastgroup, match.group(0))
            if replacement == match.group(0):
                return match.group(0)
            has_sep = match.group(0).endswith(("/", "\\"))
            return replacement + ("/" if has_sep else "")

        return pattern.sub(_sub, target)

    @staticmethod
    def path_replace_in_object(replacements, obj, ignore_key_paths=None):
        """
        Recursively apply path_string_replace to all string values in an object tree.

        Ignore behavior (current phase):
        - Supports first-level key omission only.
        - If a top-level dict key matches ignore_key_paths entry, that subtree is left unchanged.
        - No recursive key omission matching is performed.
        """
        ignore_top_level = set()
        if isinstance(ignore_key_paths, (list, tuple, set)):
            for raw in ignore_key_paths:
                if not isinstance(raw, str):
                    continue
                s = raw.strip()
                if not s:
                    continue
                # First-level support only. If dotted path is passed, use first segment.
                ignore_top_level.add(s.split(".", 1)[0])

        def _walk(v, depth: int):
            if isinstance(v, dict):
                out = {}
                for k, item in v.items():
                    k_str = str(k)
                    if depth == 0 and k_str in ignore_top_level:
                        out[k] = item
                        continue
                    out[k] = _walk(item, depth + 1)
                return out
            if isinstance(v, list):
                return [_walk(item, depth + 1) for item in v]
            if isinstance(v, str):
                return DataSession.path_string_replace(replacements, v)
            return v

        return _walk(obj, 0)

    @staticmethod
    def safe_path_file_replace(
        path: str,
        replacements,
        strict_json: bool = True,
        ignore_key_paths=None,
    ) -> dict:
        """
        Safely replace path strings in one file.

        Flow:
        1) read original
        2) build candidate via path_string_replace
        3) if original .json is valid and strict_json=True, candidate must remain valid
        4) write candidate temp, backup original, swap
        5) restore backup on any swap/verification failure
        """
        if not isinstance(path, str) or not path.strip():
            return {"status": "error", "error": "invalid_path", "updated": False}

        path_abs = os.path.abspath(path.strip())
        if not os.path.isfile(path_abs):
            return {
                "status": "error",
                "error": "path_not_file",
                "path": path_abs,
                "updated": False,
            }

        try:
            with open(path_abs, "r", encoding="utf-8") as f:
                original_text = f.read()
        except Exception as exc:
            return {
                "status": "error",
                "error": "read_failed",
                "reason": str(exc),
                "path": path_abs,
                "updated": False,
            }

        is_json_file = path_abs.lower().endswith(".json")
        original_json_valid = False
        original_json_obj = None
        if strict_json and is_json_file:
            try:
                original_json_obj = json.loads(original_text)
                original_json_valid = True
            except Exception:
                original_json_valid = False

        try:
            use_structured_json_processor = (
                is_json_file
                and original_json_valid
                and isinstance(ignore_key_paths, (list, tuple, set))
                and len(ignore_key_paths) > 0
                and isinstance(original_json_obj, (dict, list))
            )
            if use_structured_json_processor:
                candidate_obj = DataSession.path_replace_in_object(
                    replacements,
                    original_json_obj,
                    ignore_key_paths=ignore_key_paths,
                )
                candidate_text = json.dumps(
                    candidate_obj,
                    indent=2,
                    ensure_ascii=True,
                ) + "\n"
            else:
                candidate_text = DataSession.path_string_replace(
                    replacements,
                    original_text,
                )
        except Exception as exc:
            return {
                "status": "error",
                "error": "replace_failed",
                "reason": str(exc),
                "path": path_abs,
                "updated": False,
            }

        if candidate_text == original_text:
            return {
                "status": "ok",
                "path": path_abs,
                "updated": False,
                "reason": "noop",
            }

        if strict_json and is_json_file and original_json_valid:
            try:
                json.loads(candidate_text)
            except Exception as exc:
                return {
                    "status": "error",
                    "error": "candidate_json_invalid",
                    "reason": str(exc),
                    "path": path_abs,
                    "updated": False,
                }

        base = os.path.basename(path_abs)
        folder = os.path.dirname(path_abs) or "."
        candidate_path = os.path.join(folder, f".{base}.candidate")
        backup_path = os.path.join(folder, f".{base}.backup")

        try:
            if os.path.exists(candidate_path):
                os.remove(candidate_path)
            if os.path.exists(backup_path):
                os.remove(backup_path)
        except Exception:
            pass

        try:
            with open(candidate_path, "w", encoding="utf-8") as f:
                f.write(candidate_text)
        except Exception as exc:
            return {
                "status": "error",
                "error": "candidate_write_failed",
                "reason": str(exc),
                "path": path_abs,
                "updated": False,
            }

        backup_restored = False
        try:
            os.rename(path_abs, backup_path)
            os.rename(candidate_path, path_abs)

            with open(path_abs, "r", encoding="utf-8") as f:
                verify_text = f.read()
            if strict_json and is_json_file and original_json_valid:
                json.loads(verify_text)

            if os.path.exists(backup_path):
                os.remove(backup_path)

            return {
                "status": "ok",
                "path": path_abs,
                "updated": True,
                "backup_restored": False,
            }
        except Exception as exc:
            try:
                if os.path.exists(path_abs):
                    os.remove(path_abs)
            except Exception:
                pass
            try:
                if os.path.exists(backup_path):
                    os.rename(backup_path, path_abs)
                    backup_restored = True
            except Exception:
                pass
            try:
                if os.path.exists(candidate_path):
                    os.remove(candidate_path)
            except Exception:
                pass

            return {
                "status": "error",
                "error": "swap_failed",
                "reason": str(exc),
                "path": path_abs,
                "updated": False,
                "backup_restored": backup_restored,
            }

    @staticmethod
    def safe_path_file_move(src_path: str, dst_path: str) -> dict:
        if not isinstance(src_path, str) or not src_path.strip():
            return {
                "status": "error",
                "src_path": src_path,
                "dst_path": dst_path,
                "moved": False,
                "backup_restored": False,
                "reason": "invalid_src_path",
            }
        if not isinstance(dst_path, str) or not dst_path.strip():
            return {
                "status": "error",
                "src_path": src_path,
                "dst_path": dst_path,
                "moved": False,
                "backup_restored": False,
                "reason": "invalid_dst_path",
            }

        src_abs = os.path.abspath(src_path.strip())
        dst_abs = os.path.abspath(dst_path.strip())
        if not os.path.isfile(src_abs):
            return {
                "status": "error",
                "src_path": src_abs,
                "dst_path": dst_abs,
                "moved": False,
                "backup_restored": False,
                "reason": "src_not_file",
            }
        if src_abs == dst_abs:
            return {
                "status": "ok",
                "src_path": src_abs,
                "dst_path": dst_abs,
                "moved": False,
                "backup_restored": False,
                "reason": "noop",
            }
        if os.path.isdir(dst_abs):
            return {
                "status": "error",
                "src_path": src_abs,
                "dst_path": dst_abs,
                "moved": False,
                "backup_restored": False,
                "reason": "dst_is_dir",
            }

        dst_parent = os.path.dirname(dst_abs) or "."
        dst_base = os.path.basename(dst_abs)
        backup_path = os.path.join(dst_parent, f".{dst_base}.backup")
        dst_had_existing = False
        backup_restored = False

        try:
            os.makedirs(dst_parent, exist_ok=True)
            if os.path.exists(backup_path):
                os.remove(backup_path)

            dst_had_existing = os.path.exists(dst_abs)
            if dst_had_existing:
                os.rename(dst_abs, backup_path)

            os.rename(src_abs, dst_abs)
            if (not os.path.isfile(dst_abs)) or os.path.exists(src_abs):
                raise Exception(f"move_verification_failed:{dst_abs}")

            if dst_had_existing and os.path.exists(backup_path):
                os.remove(backup_path)

            return {
                "status": "ok",
                "src_path": src_abs,
                "dst_path": dst_abs,
                "moved": True,
                "backup_restored": False,
                "reason": "moved",
            }
        except Exception as exc:
            try:
                if (not os.path.exists(src_abs)) and os.path.exists(dst_abs):
                    os.rename(dst_abs, src_abs)
            except Exception:
                pass
            try:
                if dst_had_existing and os.path.exists(backup_path):
                    if os.path.exists(dst_abs):
                        os.remove(dst_abs)
                    os.rename(backup_path, dst_abs)
                    backup_restored = True
            except Exception:
                pass

            return {
                "status": "error",
                "src_path": src_abs,
                "dst_path": dst_abs,
                "moved": False,
                "backup_restored": backup_restored,
                "reason": str(exc),
            }

    @staticmethod
    def collect_paths_under_root(
        root_path: str,
        include_exts=None,
        exclude_dirs=None,
    ) -> list:
        """
        Recursively collect target file paths under root.
        Returns absolute paths in deterministic sorted order.
        """
        if not isinstance(root_path, str) or not root_path.strip():
            return []
        root_abs = os.path.abspath(root_path.strip())
        if not os.path.isdir(root_abs):
            return []

        exts = include_exts if isinstance(include_exts, (list, tuple, set)) and len(include_exts) > 0 else [".json", ".ndjson", ".txt"]
        ext_set = set()
        for e in exts:
            if not isinstance(e, str):
                continue
            s = e.strip().lower()
            if not s:
                continue
            if not s.startswith("."):
                s = "." + s
            ext_set.add(s)
        if len(ext_set) == 0:
            ext_set = {".json", ".ndjson", ".txt"}

        exclude = set()
        if isinstance(exclude_dirs, (list, tuple, set)):
            for d in exclude_dirs:
                if isinstance(d, str) and d.strip():
                    exclude.add(d.strip())

        out = []
        for dirpath, dirnames, filenames in os.walk(root_abs):
            if exclude:
                dirnames[:] = [d for d in dirnames if d not in exclude]
            dirnames.sort()
            filenames.sort()
            for name in filenames:
                ext = os.path.splitext(name)[1].lower()
                if ext not in ext_set:
                    continue
                out.append(os.path.abspath(os.path.join(dirpath, name)))
        return out

    @staticmethod
    def safe_path_file_replace_many(
        target_paths,
        replacements,
        ignore_key_paths=None,
        stop_on_error: bool = False,
    ) -> dict:
        """
        Run safe_path_file_replace over an explicit list of target files.
        """
        if not isinstance(target_paths, (list, tuple)) or len(target_paths) == 0:
            return {
                "status": "ok",
                "files_total": 0,
                "files_scanned": 0,
                "files_updated": 0,
                "files_noop": 0,
                "files_failed": 0,
                "updated_paths": [],
                "noop_paths": [],
                "failed": [],
                "backup_restored_count": 0,
            }

        updated_paths = []
        noop_paths = []
        failed = []
        backup_restored_count = 0
        files_scanned = 0
        files_updated = 0
        files_noop = 0
        files_failed = 0

        for raw_path in target_paths:
            if not isinstance(raw_path, str) or not raw_path.strip():
                continue
            files_scanned += 1
            p = os.path.abspath(raw_path.strip())
            res = DataSession.safe_path_file_replace(
                p,
                replacements,
                ignore_key_paths=ignore_key_paths,
            )
            if bool(res.get("backup_restored")):
                backup_restored_count += 1

            if str(res.get("status", "")).lower() == "error":
                files_failed += 1
                failed.append({
                    "path": p,
                    "error": res.get("error"),
                    "reason": res.get("reason"),
                })
                if stop_on_error:
                    break
                continue

            if bool(res.get("updated")):
                files_updated += 1
                updated_paths.append(p)
            else:
                files_noop += 1
                noop_paths.append(p)

        status = "ok" if files_failed == 0 else "error"
        return {
            "status": status,
            "files_total": len(target_paths),
            "files_scanned": files_scanned,
            "files_updated": files_updated,
            "files_noop": files_noop,
            "files_failed": files_failed,
            "updated_paths": updated_paths,
            "noop_paths": noop_paths,
            "failed": failed,
            "backup_restored_count": backup_restored_count,
        }

    @staticmethod
    def safe_path_replace_under_root(
        root_path: str,
        replacements,
        include_exts=None,
        ignore_key_paths=None,
        stop_on_error: bool = False,
        exclude_dirs=None,
    ) -> dict:
        """
        Discover target files under root and run safe_path_file_replace_many.
        """
        if not isinstance(root_path, str) or not root_path.strip():
            return {"status": "error", "error": "invalid_root_path"}
        root_abs = os.path.abspath(root_path.strip())
        if not os.path.isdir(root_abs):
            return {"status": "error", "error": "root_not_dir", "root_path": root_abs}

        matched_paths = DataSession.collect_paths_under_root(
            root_path=root_abs,
            include_exts=include_exts,
            exclude_dirs=exclude_dirs,
        )
        out = DataSession.safe_path_file_replace_many(
            target_paths=matched_paths,
            replacements=replacements,
            ignore_key_paths=ignore_key_paths,
            stop_on_error=stop_on_error,
        )
        out["root_path"] = root_abs
        out["matched_count"] = len(matched_paths)
        return out

    def exists(self, path: str) -> bool:
        if not self.is_contained_path(path):
            raise Exception(f"7 Not a contained path {path}. You should be using a relative ../ path. is not in {self[self.f_root]}")
        assert isinstance(path, str) and path, f"Must supply a non-empty path: {path} "
        full_path = os.path.join(self[self.f_root], path)
        real = os.path.abspath(os.path.normpath(full_path))
        if ( self[self.f_verbose] == True):
            print("ds_exists -> "+full_path)
            print("ds_exists norm -> "+real)
        return os.path.exists(real)
    # A -> /Users/computercomputer/justinops/ai_services/ai_services/nodes/temp/weekly_schedule_factory_tests/test_reuse_schedule_across_nodes_advances_with_multiple_days/shared_store/schedule.json

    def delete(self, path: str) -> bool:
        if not self.is_contained_path(path):
            raise Exception(f"6 Not a contained path {path}. You should be using a relative ../ path.  is not in {self[self.f_root]}")
        assert isinstance(path, str) and path, "Must supply a non-empty path"
        full_path = os.path.join(self[self.f_root], path)
        if os.path.isdir(full_path):
            try:
                os.rmdir(full_path)
            except OSError:
                shutil.rmtree(full_path)
        else:
            os.remove(full_path)
        return True

    def open(self, path: str, mode: str = 'r'):
        if not self.is_contained_path(path):
            raise Exception(f"5 Not a contained path {path}. You should be using a relative ../ path.  is not in {self[self.f_root]}")

        assert isinstance(path, str) and len(path) > 0
        assert isinstance(mode, str) and mode in ['r', 'rb', 'w', 'wb']
        full_path = os.path.join(self[self.f_root], path)
        if ( self[self.f_verbose] == True):
            print("data_session open ..."+full_path)

        if mode in ['w', 'wb']:        
            directory = os.path.dirname(full_path)
            os.makedirs(directory, exist_ok=True)      
        # print(full_path)  
        if ( self[self.f_verbose] == True):
            if mode in ['r', 'rb']:
                print("ds loading ..."+full_path)
            if mode in [ 'w', 'wb']:
                print("ds writing ..."+full_path)
        real = os.path.abspath(os.path.normpath(full_path))
        return open(real, mode)

    def listdir(self, path: str):
        if not self.is_contained_path(path):
            raise Exception(f"4 Not a contained path {path}. You should be using a relative ../ path.  is not in {self[self.f_root]}")

        """Lists the contents of the directory at the given path."""
        assert isinstance(path, str) and path, "Must supply a non-empty path"
        full_path = os.path.join(self[self.f_root], path)
        #print(f"Attempting to access: {full_path}")
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"No such directory: '{path}' in { os.getcwd()}")

        if not os.path.isdir(full_path):
            raise NotADirectoryError(f"Path is not a directory: '{path}'")
        return os.listdir(full_path)

    def isfile(self, path: str) -> bool:
        if not self.is_contained_path(path):
            raise Exception(f"3 Not a contained path {path}. You should be using a relative ../ path.  is not in {self[self.f_root]}")

        full_path = os.path.join(self[self.f_root], path)
        return os.path.isfile(full_path)

    def isfolder(self, path: str) -> bool:
        if not self.is_contained_path(path):
            raise Exception(f"2 Not a contained path {path}. You should be using a relative ../ path.  is not in {self[self.f_root]}")

        full_path = os.path.join(self[self.f_root], path)
        return os.path.isdir(full_path)
    
    def mkdir(self, path: str, exist_ok: bool = True) -> None:
        if not self.is_contained_path(path):
            raise Exception(f"1 Not a contained path {path}. You should be using a relative ../ path.")

        assert isinstance(path, str) and path, "Must supply a non-empty path"
        full_path = os.path.join(self[self.f_root], path)
        os.makedirs(full_path, exist_ok=exist_ok)

    def move(self, src: str, dst: str, overwrite: bool = True) -> None:
        assert isinstance(src, str) and src, "Must supply a non-empty src"
        assert isinstance(dst, str) and dst, "Must supply a non-empty dst"
        if not self.is_contained_path(src) or not self.is_contained_path(dst):
            raise Exception("Not a contained path")
        if not self.exists(src):
            raise FileNotFoundError(src)
        if (overwrite is not True) and self.exists(dst):
            raise Exception("destination exists")
        if overwrite is True and self.exists(dst):
            self.delete(dst)
        parent = os.path.dirname(dst)
        if parent and parent not in (".", "/"):
            self.mkdir(parent, exist_ok=True)
        shutil.move(self.to_local_path(src), self.to_local_path(dst))

    def copy(self, src: str, dst: str, overwrite: bool = True) -> None:
        assert isinstance(src, str) and src, "Must supply a non-empty src"
        assert isinstance(dst, str) and dst, "Must supply a non-empty dst"
        if not self.is_contained_path(src) or not self.is_contained_path(dst):
            raise Exception("Not a contained path")
        if not self.exists(src):
            raise FileNotFoundError(src)
        if (overwrite is not True) and self.exists(dst):
            raise Exception("destination exists")
        if overwrite is True and self.exists(dst):
            self.delete(dst)
        parent = os.path.dirname(dst)
        if parent and parent not in (".", "/"):
            self.mkdir(parent, exist_ok=True)
        if self.isfolder(src):
            shutil.copytree(self.to_local_path(src), self.to_local_path(dst))
        else:
            shutil.copy2(self.to_local_path(src), self.to_local_path(dst))


    def get_root(self):
        return self[self.f_root]
'''
TODO -- upgrade to sit on top of fsspec!

    # --- fsspec compatibility layer ---
    protocol = "file"

    def _open(self, path, mode='rb', **kwargs):
        """
        fsspec calls _open; we delegate to our open().
        """
        return self.open(path, mode)

    def ls(self, path, detail=False):
        """
        List files under `path`.  If detail=True, return list of dicts
        with at least name/type/size.
        """
        try:
            entries = self.listdir(path)
        except FileNotFoundError:
            # if it's a file, return [path]
            if self.isfile(path):
                entries = [path]
            else:
                raise
        if detail:
            out = []
            for name in entries:
                # build full logical path
                p = path.rstrip("/") + "/" + name if self.isdir(path) else name
                out.append({
                    "name": p,
                    "type": "directory" if self.isdir(p) else "file",
                    "size": None,  # you could stat(p) here if you want
                })
            return out
        return entries

    def info(self, path):
        """
        Return a single-entry detailed dict for `path`.
        """
        lst = self.ls(path, detail=True)
        if not lst:
            raise FileNotFoundError(path)
        return lst[0]

    # aliases for fsspec
    isdir    = isfolder
    rm_file  = delete
    makedirs = mkdir

    def rmdir(self, path, recursive=False):
        """
        Remove a (possibly non-empty) directory.  
        """
        full = os.path.join(self[self.f_root], path)
        if recursive:
            shutil.rmtree(full)
        else:
            os.rmdir(full)



'''
