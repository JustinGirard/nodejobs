# ex_testservice_mcp_tests.py
import asyncio
import socket
import sys
import time
import unittest
from nodejobs.jobs import Jobs, JobRecord
from fastmcp import Client
from nodejobs.dependencies.TestService import TestService


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class TestServiceMCPTests(unittest.TestCase):

    def test_now_direct(self):
        out = TestService.now(tz_offset=1)
        self.assertEqual(out, 11.0)

    def test_now_cli_subprocess(self):
        jobs = Jobs(db_path="./test_nodejobs_db")
        job_id = "testservice_cli"
        cmd = [
            sys.executable, "-m", "nodejobs.dependencies.TestService",
            "now", "tz_offset=1",
        ]
        jobs.run(command=cmd, job_id=job_id)

        timeout = time.time() + 5
        while jobs.get_status(job_id).status not in (JobRecord.Status.c_finished, JobRecord.Status.c_failed):
            self.assertTrue(time.time() < timeout, "CLI did not finish in time")
            time.sleep(0.1)

        stdout_text, stderr_text = jobs.job_logs(job_id)
        self.assertIn("11", stdout_text)
        self.assertEqual("", stderr_text or "")

    def test_now_mcp_http(self):
        jobs = Jobs(db_path="./test_nodejobs_db")
        host, port, path = "127.0.0.1", _free_port(), "/mcp"
        job_id = "testservice_mcp_http"

        cmd = [
            sys.executable, "ex_mcp_service.py",
            "serve",
            "transport=http",
            f"host={host}",
            f"port={port}",
            f"path={path}",
        ]
        jobs.run(command=cmd, job_id=job_id)

        timeout = time.time() + 5
        while jobs.get_status(job_id).status == JobRecord.Status.c_starting:
            self.assertTrue(time.time() < timeout, "Server failed to start in time")
            time.sleep(0.1)

        async def _talk():
            client = Client(f"http://{host}:{port}{path}")
            async with client:
                await client.ping()
                call_tool_result = await client.call_tool("now", {"tz_offset": 1})
                self.assertEqual(float(call_tool_result.data), 11.0)
        try:
            pass
            asyncio.run(_talk())
        finally:
            jobs.stop(job_id)
        timeout = time.time() + 15
        job_status = jobs.get_status(job_id).status
        while job_status not in (JobRecord.Status.c_finished, JobRecord.Status.c_stopped, JobRecord.Status.c_failed):
            print(f"current_status: {job_status}")
            self.assertTrue(JobRecord.Status.c_stopped == job_status, "Should report stopped")
            self.assertTrue(time.time() < timeout, "Server failed to stop in time")
            time.sleep(0.1)


if __name__ == "__main__":
    # cd ..
    unittest.main()
    # unittest.main(defaultTest="TestServiceMCPTests.test_now_direct")
    # unittest.main(defaultTest="TestServiceMCPTests.test_now_cli_subprocess")
    # unittest.main(defaultTest="TestServiceMCPTests.test_now_mcp_http")
