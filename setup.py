import os
import pathlib

from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext as build_ext_orig


# class CMakeExtension(Extension):

#     def __init__(self, name):
#         # don't invoke the original build_ext for this special extension
#         super().__init__(name, sources=[])


# class build_ext(build_ext_orig):

#     def run(self):
#         for ext in self.extensions:
#             self.build_cmake(ext)
#         super().run()

#     def build_cmake(self, ext):
#         cwd = pathlib.Path().absolute()

#         # these dirs will be created in build_py, so if you don't have
#         # any python sources to bundle, the dirs will be missing
#         build_temp = pathlib.Path(self.build_temp)
#         build_temp.mkdir(parents=True, exist_ok=True)
#         extdir = pathlib.Path(self.get_ext_fullpath(ext.name))
#         extdir.mkdir(parents=True, exist_ok=True)

#         # example of cmake args
#         config = 'Debug' if self.debug else 'Release'
#         cmake_args = [
#             # '-DCMAKE_LIBRARY_OUTPUT_DIRECTORY=' + str(extdir.parent.absolute()),
#             '-DCMAKE_BUILD_TYPE=' + config,
#             '-DPYTARGET_OUTPUT_DIR='+ str(extdir.parent.absolute()),
#             '-DPYTHON_SETUP=ON'
#         ]

#         # example of build args
#         build_args = [
#             '--config', config,
#             '--', '-j4',
#         ]

#         os.chdir(str(build_temp))
#         self.spawn(['cmake', str(cwd)] + cmake_args)
#         if not self.dry_run:
#             self.spawn(['cmake', '--build', '.'] + build_args)
#         os.chdir(str(cwd))

import os
import re
import sys
import sysconfig
import platform
import subprocess
from pathlib import Path

from distutils.version import LooseVersion
from setuptools import setup, Extension, find_packages
from setuptools.command.build_ext import build_ext
from setuptools.command.test import test as TestCommand


class CMakeExtension(Extension):
    def __init__(self, name):
        Extension.__init__(self, name, sources=[])


class CMakeBuild(build_ext):
    def run(self):
        try:
            out = subprocess.check_output(['cmake', '--version'])
        except OSError:
            raise RuntimeError(
                "CMake must be installed to build the following extensions: " +
                ", ".join(e.name for e in self.extensions))
        env = os.environ.copy()
        build_directory = os.path.abspath(self.build_temp)

        cmake_args = [
            '-DCMAKE_LIBRARY_OUTPUT_DIRECTORY=' + build_directory,
            '-DPYTHON_EXECUTABLE=' + sys.executable,
            # '-DCMAKE_INSTALL_PREFIX=' + env['CONDA_PREFIX'],
            '-DPYTHON_SETUP=ON'
        ]

        cfg = 'Debug' if self.debug else 'Release'
        build_args = ['--config', cfg]

        cmake_args += ['-DCMAKE_BUILD_TYPE=' + cfg]

        # Assuming Makefiles
        build_args += ['--', '-j2']

        self.build_args = build_args


        # env['CXXFLAGS'] = '{} -DVERSION_INFO=\\"{}\\"'.format(
        #     env.get('CXXFLAGS', ''),
        #     self.distribution.get_version())
        if not os.path.exists(self.build_temp):
            os.makedirs(self.build_temp)

        # CMakeLists.txt is in the same directory as this setup.py file
        cmake_list_dir = os.path.abspath(os.path.dirname(__file__))
        print('-'*10, 'Running CMake prepare', '-'*40)
        subprocess.check_call(['cmake', cmake_list_dir] + cmake_args,
                              cwd=self.build_temp, env=env)

        print('-'*10, 'Building extensions', '-'*40)
        cmake_cmd = ['cmake', '--build', '.'] + self.build_args
        subprocess.check_call(cmake_cmd,
                              cwd=self.build_temp)

        print('-'*10, 'Installing cpp library', '-'*40)
        cmake_cmd = ['make', 'install']
        subprocess.check_call(cmake_cmd,
                              cwd=self.build_temp)
        # Move from build temp to final position
        for ext in self.extensions:
            self.move_output(ext)

    def move_output(self, ext):
        build_temp = Path(self.build_temp).resolve()
        dest_path = Path(self.get_ext_fullpath(ext.name)).resolve()
        source_path = build_temp / self.get_ext_filename(ext.name)
        dest_directory = dest_path.parents[0]
        dest_directory.mkdir(parents=True, exist_ok=True)
        self.copy_file(source_path, dest_path)



from setuptools import setup, find_packages

install_requires = ["numpy"]

setup(
    name="pyicf",

    version="0.1.0",
    description="An indexable container file format.",
    author="Samuel Flis",
    author_email="samuel.d.flis@gmail.com",
    url="https://github.com/sflis/pyicf",
    packages=find_packages(),
    provides=["icf"],
    license="GNU Lesser General Public License v3 or later",
    install_requires=install_requires,
    ext_modules=[CMakeExtension('icf/_ext/icf_py')],
    cmdclass={
        'build_ext': CMakeBuild,
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