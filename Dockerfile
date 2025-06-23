# This is just a quick docker container used by JG (and maybe you) to test the system install and unit tests
# Its not meant to be automated or do anything other than facilitate fiddling / encapsulation
# Use Debian-based Python
FROM python:3.11-slim-bullseye

# Install git for pip-installing from GitHub
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# Upgrade pip and install pytest (or your test runner)
RUN pip install --upgrade pip setuptools wheel pytest

# Install your repo from GitHub (replace URL as needed)
RUN pip install git+https://github.com/yourusername/yourrepo.git@main

# Run tests, then keep the container alive
CMD ["bash", "-lc", "pytest && sleep infinity"]
