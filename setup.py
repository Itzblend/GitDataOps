from setuptools import setup, find_packages

with open("requirements.txt") as f:
    required = f.read().splitlines()

with open("README.md") as f:
    long_description = f.read()

setup(
    name="git_data_ops",
    version="0.1.0",
    author="Your Name",
    author_email="lauri@datanuggets.io",
    description="Git DataOps Python library",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Itzblend/GitDataOps",
    packages=find_packages(),  # Automatically discover and include all packages
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=required,
    python_requires=">=3.8",  # Specify the minimum Python version required
    entry_points={
        "console_scripts": [
            "gitdataops=git_data_ops.cli:main",
        ]
    },
)
