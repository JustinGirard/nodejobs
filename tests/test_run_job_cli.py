import unittest
from types import SimpleNamespace
from unittest.mock import patch

from nodejobs.run_job import RunJob


DEBUG = True


def dprint(*args):
    if DEBUG:
        print(*args)


class TestRunJobCli(unittest.TestCase):
    def test_cli_parent_is_not_an_existing_job(self):
        job = object.__new__(RunJob)
        job.job_id = "cli-parent-test"
        parent = SimpleNamespace(
            pid=123,
            info={"cmdline": [
                "python3", "-m", "nodejobs.jobs_service", "run",
                "job_id=cli-parent-test",
            ]},
        )
        with patch("nodejobs.run_job.os.getppid", return_value=123), \
                patch("nodejobs.run_job.psutil.process_iter", return_value=[parent]):
            self.assertIsNone(job.find_existing())
        dprint("PROOF CLI parent: launcher is not treated as an existing job")

    def test_other_matching_process_still_blocks(self):
        job = object.__new__(RunJob)
        job.job_id = "cli-parent-test"
        parent = SimpleNamespace(
            pid=123,
            info={"cmdline": [
                "python3", "-m", "nodejobs.jobs_service", "run",
                "job_id=cli-parent-test",
            ]},
        )
        other = SimpleNamespace(
            pid=456,
            info={"cmdline": ["bash", "-c", "sleep 30 # cli-parent-test"]},
        )
        with patch("nodejobs.run_job.os.getppid", return_value=123), \
                patch("nodejobs.run_job.psutil.process_iter", return_value=[parent, other]):
            self.assertIs(job.find_existing(), other)
        dprint("PROOF duplicate job: separate matching process is still detected")


if __name__ == "__main__":
    unittest.main()
