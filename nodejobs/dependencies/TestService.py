# TestService.py
from decelium_wallet.commands.BaseService import BaseService
import json
from http.server import BaseHTTPRequestHandler, HTTPServer

'''
The smallest way to create a CLI service and registry

'''
class TestService(BaseService):
    @classmethod
    def get_command_map(cls):
        return {
            "launch": {"required_args": [], "method": cls.launch},
            "serve": {"required_args": [], "method": cls.serve},
            "now": {"required_args": ["tz_offset"], "method": cls.now},
        }

    @classmethod
    def now(cls, tz_offset: float) -> float:
        return float(tz_offset) + 10

    @classmethod
    def launch(
        cls,
        host: str = "127.0.0.1",
        port: int = 8765,
        path: str = "/cmd",
    ):
        route = str(path)
        if not route.startswith("/"):
            raise Exception("path_must_start_with_slash")

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt, *args):
                return

            def do_POST(self):
                if self.path != route:
                    body = b"Not Found"
                    self.send_response(404)
                    self.send_header("content-type", "text/plain")
                    self.send_header("content-length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                try:
                    raw_len = int(self.headers.get("content-length", "0"))
                    raw = self.rfile.read(raw_len).decode("utf-8") if raw_len > 0 else "{}"
                    payload = json.loads(raw)
                    if not isinstance(payload, dict):
                        raise Exception("payload_must_be_object")
                    command_id = payload.get("command_id")
                    data = payload.get("data")
                    if not isinstance(command_id, str) or len(command_id.strip()) == 0:
                        raise Exception("command_id_required")
                    if not isinstance(data, dict):
                        raise Exception("data_required")
                    result = cls.run(__command=[command_id], **data)
                    body = json.dumps(result).encode("utf-8")
                    self.send_response(201)
                    self.send_header("content-type", "application/json")
                    self.send_header("content-length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except Exception as exc:
                    body = json.dumps({"error": str(exc)}).encode("utf-8")
                    self.send_response(500)
                    self.send_header("content-type", "application/json")
                    self.send_header("content-length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)

        server = HTTPServer((str(host), int(port)), Handler)
        server.serve_forever()

    @classmethod
    def serve(cls, transport: str = "http", host: str = "127.0.0.1", port: int = 8765, path: str = "/mcp"):
        from fastmcp import FastMCP
        mcp = FastMCP("test-service")

        @mcp.tool(name="now")
        def now(tz_offset: float) -> float:
            return cls.now(tz_offset=tz_offset)

        mcp.run(transport=str(transport), host=str(host), port=int(port), path=str(path))

if __name__ == "__main__":
    TestService.run_cli()
# python3 TestService.py now tz_offset=1

# 1 - Get rid of get_command_map
# 2 - Get MCP running
# 3 - update BaseService and Wallet 
