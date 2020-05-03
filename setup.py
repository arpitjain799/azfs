import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()


def _requires_from_file(filename):
    return open(filename).read().splitlines()


setuptools.setup(
    name="azfs",
    version="0.1.2",
    author="gsy0911",
    author_email="yoshiki0911@gmail.com",
    description="AzFS is to provide convenient Python read/write functions for Azure Storage Account.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/gsy0911/pystock",
    packages=setuptools.find_packages(),
    install_requires=_requires_from_file('requirements.txt'),
    license="MIT",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)