import setuptools
import os


def get_version(version_tuple):
    return ".".join(map(str, version_tuple))


init = os.path.join(
    os.path.dirname(__file__), 'azfs', '__init__.py'
)

version_line = list(
    filter(lambda l: l.startswith('VERSION'), open(init))
)[0]

# eval is required to convert from string to tuple,
# because VERSION defined in __init__.py is tuple
VERSION = get_version(eval(version_line.split('=')[-1]))

with open("README.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(
    name="azfs",
    version="0.1.5",
    author="gsy0911",
    author_email="yoshiki0911@gmail.com",
    description="AzFS is to provide convenient Python read/write functions for Azure Storage Account.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/gsy0911/azfs",
    packages=setuptools.find_packages(),
    install_requires=[
        "pandas>=1.0.0",
        "azure-identity>=1.3.1",
        "azure-storage-blob>=12.3.0",
        "azure-storage-file-datalake>=12.0.0",
        "azure-storage-queue>=12.1.1"
    ],
    license="MIT",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
