

from typing import Any, List, Dict, Optional
import json, hashlib
from nodejobs.dependencies.CachingComponent import CachingComponent
from nodejobs.jobs import Jobs
from nodejobs.jobdb import JobRecord
import time

class FieldSet(CachingComponent):
    # _TYPES = {"str": str, "int": int, "float": float, "bool": bool, "dict": dict, "list": list,"file":str,"directory":str}
    # _LOAD_EXTENSIONS = ["txt","cs","js","json","yaml","html","css","py"]
    _TYPES = {
        "str": str,
        "int": int,
        "float": float,
        "bool": bool,
        "dict": dict,
        "dict_row": dict,
        "json_list": (list, dict, str),
        "dicttable": (list, dict, str),
        "string_search_select": str,
        "string_select": str,
        "rgba": str,
        "string_select_multiple": list,
        "list": list,
        "file": str,
        "file_editor": str,
        "directory": str,
        "image_path": str,
    }

    _LOAD_EXTENSIONS = [
        "txt",
        "cs",
        "js",
        "json",
        "yaml",
        "html",
        "css",
        "py",
        "csv",
        "md"
    ]

    _KEY_IN_JSON = "_in_json"
    _KEY_AS_JSON = "_as_json"

    #def get_fields(self,query=None) -> List[Any]:
    #    return self.load_cache("fields")
    
    def get_data(self) -> List[Any]:
        return self.load_cache("values")


    def _set_fields(self, query: Dict[str, Any]) -> List[Any]:
        # query: {field_id: "str"}  OR  {field_id: {"type": "str"}}
        try:
            if not isinstance(query, dict):
                return {"error": "fields must be a dict"}

            fields: Dict[str, Dict[str, str]] = {}
            for k, v in query.items():
                t = v.get("type")
                if t not in self._TYPES:
                    return {"error": f"invalid type for '{k}': {t}"}
                fields[str(k)] = v

            self.save_cache("fields", fields)
            return   self.load_cache("fields")
        
        except Exception as e:
            import traceback as tb
            return {"error": str(e),
                    "traceback":tb.format_exc()}

    def _get_default_fields(self):
        default_fields= {
        self._KEY_IN_JSON: {"type": "json_list", "optional": True},
        # "data_dir": {"type":"str","optional":True},
        # "cwd":{"type":"directory","optional":True, "default":"./work"},
        # "command":{"type":"file"},
        # "overwrite":{"type":"bool","optional":True},
        # "job_id":{"type":"str","optional":True},
        }
        return default_fields

    def _apply_in_json(self, query: Dict[str, Any]) -> None:
        incoming_json = query.pop(self._KEY_IN_JSON, None)
        if incoming_json is None: return
        if isinstance(incoming_json, list):
            if not incoming_json: return
            incoming_json = incoming_json[0]
        if isinstance(incoming_json, str): incoming_json = json.loads(incoming_json)
        if not isinstance(incoming_json, dict): raise ValueError("_in_json must be dict or JSON object string")
        for field_name, field_value in incoming_json.items():
            if field_name not in query or query[field_name] is None: query[field_name] = field_value
    
    def get_fields(self,query=None) -> List[Any]:
        if isinstance(query, dict) and query.get("overwrite") is True:
            res = self.set_fields(self._get_default_fields())
            if isinstance(res, dict) and "error" in res:
                return res
        try:
            fields = self.load_cache("fields")
        except Exception:
            fields = None

        if not isinstance(fields, dict) or not fields:

            res = self.set_fields(self._get_default_fields())
            if isinstance(res, dict) and "error" in res:
                return res
            fields = self.load_cache("fields")
        if isinstance(fields, dict) and fields:
            default_fields = self._get_default_fields()
            if default_fields:
                missing_names = [name for name in default_fields.keys() if name not in fields]
                if missing_names:
                    for name in missing_names: fields[name] = default_fields[name]
                    update_result = self.set_fields(fields)
                    if isinstance(update_result, dict) and "error" in update_result: return update_result
                    fields = update_result
        return fields
        

    def set_fields(self, query: Dict[str, Any]) -> Any:
        '''Merge in default fields, then call the internal set'''
        merged = dict(query or {})

        for name, spec in merged.items():
            if name in self._get_default_fields():
                continue
            if isinstance(spec, dict):
                if "type" in spec and "arg_format" not in spec:
                    spec["arg_format"] = "-<{argname}>=<{value}>"

        merged.update(self._get_default_fields())  
        return self._set_fields(merged)
            


    def set_data(self,  **kwargs: Dict[str, Any]) -> List[Any]:
        query = kwargs
        load_paths = False
        # datSess = self.get_full_data_session()
        if "load_paths" in self and self["load_paths"] == True:                   
            load_paths = True
        try:
            # Try to load the field definitions from cache.
            # Assumes set_fields saved them under "fields".
            try:
                fields = self.load_cache("fields")
            except Exception:
                fields = None

            self._apply_in_json(query)

            # If we have a schema, enforce simple type checks.
            if isinstance(fields, dict):
                for name, value in query.items():
                    spec = fields.get(name) or fields.get(str(name))
                    if not spec:
                        # Field not defined in schema → allow it.
                        continue
                    
                    t = spec.get("type") if isinstance(spec, dict) else spec
                    py_type = self._TYPES.get(t)
                    if py_type is None:
                        return {"error": f"invalid type for '{name}': {t}"}
                    if isinstance(value, list) and py_type is not list:
                        if len(value) == 0:
                            return {"error": f"field '{name}' expected {t}, got empty list"}
                        value = value[0]
                        query[name] = value  
                    if not isinstance(value, py_type) and value != None:
                        return {
                            "error": f"field '{name}' expected {t}, got {type(value).__name__}"
                        }
            if load_paths == True:
                #return {"test":"SHOULD HAVE LOADED"}                                  
                for k, v in query.items():
                    if isinstance(v, str) and any(v.endswith("." + ext) for ext in self._LOAD_EXTENSIONS):
                        with open(v, "r", encoding="utf-8") as f:
                            query[k] = f.read()
                    else:
                        query[k] = "cannot load: value is not a string" if not isinstance(v, str) else f"cannot load: unsupported file extension "

            # Either no schema or everything validated: save values as one dict.

            query["_as_json"] = json.loads(json.dumps(query))
            self.save_cache("values", query)

            # Output is just the same input dict repeated.
            return self.get_data()

        except Exception as e:
            return {"error": str(e)}
