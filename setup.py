import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="hive_functions",
    version="0.0.8",
    author="Bee Group",
    description="Functions to work with hive",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    packages=setuptools.find_packages()
)