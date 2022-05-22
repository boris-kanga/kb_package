from setuptools import setup, find_packages

VERSION = '0.0.1'
DESCRIPTION = 'KB Packages'
LONG_DESCRIPTION = 'My personal packages for all I want to do.'

# Setting up
setup(
    # the name must match the folder name 'verysimplemodule'
    name="kb_package",
    version=VERSION,
    author="Boris KANGA",
    author_email="kangaborisparfait@gmail.com",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    packages=find_packages(),
    install_requires=[[r.strip() for r in
                       open("requirements.txt").readlines()]],
    python_requires=">=3.6",
)
