from setuptools import setup
import os

VERSION = "0.2.2.1"


def get_long_description():
    with open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"),
        encoding="utf8",
    ) as fp:
        return fp.read()


setup(
    name="ibmdbpy4nps",
    description="ibmdbpy4nps is now nzpyida",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    version=VERSION,
    install_requires=["nzpyida"],
    classifiers=["Development Status :: 7 - Inactive"],
)
