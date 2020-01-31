from setuptools import setup, find_packages
import os
import sys

install_requires = ["numpy", "pyparsing", "pyyaml", "protobuf"]

#
# from shutil import copyfile, rmtree

# if not os.path.exists("tmps"):
#     os.makedirs("tmps")
# copyfile("icf/version.py", "tmps/version.py")
# __import__("tmps.version")
# package = sys.modules["tmps"]
# package.version.update_release_version("icf")

setup(
    name="pyicf",
    version="",#,package.version.get_version(pep440=True),
    description="An indexable container file format.",
    author="Samuel Flis",
    author_email="samuel.d.flis@gmail.com",
    url="https://github.com/sflis/SSDAQ",
    packages=find_packages(),
    provides=["icf"],
    license="GNU Lesser General Public License v3 or later",
    install_requires=install_requires,
    extras_requires={
        #'encryption': ['cryptography']
    },
    # package_dir = {
    #         '': '${CMAKE_CURRENT_BINARY_DIR}'
    #   },
      package_data = {
        '': ['icf_py.cpython-37m-x86_64-linux-gnu.so']
      },
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    # entry_points={
    #     "console_scripts": [
    #     ]
    # },
)


# rmtree("tmps")
