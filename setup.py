from setuptools import setup, find_packages

setup(
    name="nodejobs",
    version="0.1.0",
    description="A simple job‐management library with Processes, JobDB, and Jobs classes",
    author="Justin Girard",
    author_email="justingirard@decelium.com",
    url="https://github.com/Decelium/nodejobs",  # adjust as needed
    license="None",
    packages=find_packages(exclude=["tests*", "docs*"]),
    python_requires=">=3.11",
    install_requires=[
        # List runtime dependencies here, for example:
        # "psutil>=5.8.0",
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: System :: Monitoring",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
    ],
    entry_points={
        # If you want to expose a console script, e.g. `nodejobs-run`
        # Uncomment and adjust the following:
        #
        # "console_scripts": [
        #     "nodejobs-run = nodejobs.jobs:main",
        # ],
    },
    include_package_data=True,
    zip_safe=False,
)
