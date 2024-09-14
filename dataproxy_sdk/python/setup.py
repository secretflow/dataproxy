# Copyright 2024 Ant Group Co., Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import platform
import re
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path

from setuptools import Extension, setup, Distribution, find_packages
from setuptools.command.build_ext import build_ext

BAZEL_MAX_JOBS = os.getenv("BAZEL_MAX_JOBS")
ROOT_DIR = os.path.dirname(__file__)
SKIP_BAZEL_CLEAN = os.getenv("SKIP_BAZEL_CLEAN")
BAZEL_CACHE_DIR = os.getenv("BAZEL_CACHE_DIR")
BAZEL_BIN = "../../bazel-bin/"


# Calls Bazel in PATH
def bazel_invoke(invoker, cmdline, *args, **kwargs):
    try:
        result = invoker(["bazel"] + cmdline, *args, **kwargs)
        return result
    except IOError:
        raise


# NOTE: The lists below must be kept in sync with dataproxy/BUILD.bazel.
ops_lib_files = [BAZEL_BIN + "dataproxy_sdk/python/dataproxy/libdataproxy.so"]

# These are the directories where automatically generated Python protobuf
# bindings are created.
generated_python_directories = [
    BAZEL_BIN + "dataproxy_sdk/proto",
]


def remove_prefix(text, prefix):
    return text[text.startswith(prefix) and len(prefix) :]


def copy_file(target_dir, filename, rootdir):
    source = os.path.relpath(filename, rootdir)
    if source.startswith(BAZEL_BIN + "dataproxy_sdk/python"):
        destination = os.path.join(
            target_dir, remove_prefix(source, BAZEL_BIN + "dataproxy_sdk/python/")
        )
    else:
        destination = os.path.join(target_dir, remove_prefix(source, BAZEL_BIN))

    # Create the target directory if it doesn't already exist.
    print(f"Create dir {os.path.dirname(destination)}")
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    if not os.path.exists(destination):
        print(f"Copy file from {source} to {destination}")
        shutil.copy(source, destination, follow_symlinks=True)
        return 1
    return 0


class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True


class BazelExtension(Extension):
    def __init__(self, name: str, sourcedir: str = "") -> None:
        super().__init__(name, sources=[])
        self.sourcedir = os.fspath(Path(sourcedir).resolve())


class BazelBuild(build_ext):
    def build_extension(self, ext: BazelExtension) -> None:
        bazel_env = dict(os.environ, PYTHON3_BIN_PATH=sys.executable)

        bazel_flags = ["--verbose_failures"]
        if BAZEL_MAX_JOBS:
            n = int(BAZEL_MAX_JOBS)  # the value must be an int
            bazel_flags.append("--jobs")
            bazel_flags.append(f"{n}")
        if BAZEL_CACHE_DIR:
            bazel_flags.append(f"--repository_cache={BAZEL_CACHE_DIR}")

        bazel_precmd_flags = []

        bazel_targets = ["//dataproxy_sdk/python/dataproxy:init"]

        bazel_flags.extend(["-c", "opt"])

        if platform.machine() == "x86_64":
            bazel_flags.extend(["--config=avx"])

        bazel_invoke(
            subprocess.check_call,
            bazel_precmd_flags + ["build"] + bazel_flags + ["--"] + bazel_targets,
            env=bazel_env,
        )

        copied_files = 0
        files_to_copy = ops_lib_files

        # Copy over the autogenerated protobuf Python bindings.
        for directory in generated_python_directories:
            for filename in os.listdir(directory):
                if filename[-3:] == ".py":
                    files_to_copy.append(os.path.join(directory, filename))

        for filename in files_to_copy:
            copied_files += copy_file(self.build_lib, filename, ROOT_DIR)
        print("{} of files copied to {}".format(copied_files, self.build_lib))


# Ensure no remaining lib files.
build_dir = os.path.join(ROOT_DIR, "build")
if os.path.isdir(build_dir):
    shutil.rmtree(build_dir)


if not SKIP_BAZEL_CLEAN:
    bazel_invoke(subprocess.check_call, ["clean"])


# Default Linux platform tag
plat_name = "manylinux2014_x86_64"
if sys.platform == "darwin":
    # Due to a bug in conda x64 python, platform tag has to be 10_16 for X64 wheel
    if platform.machine() == "x86_64":
        plat_name = "macosx_10_16_x86_64"
    else:
        plat_name = "macosx_12_0_arm64"
elif platform.machine() == "aarch64":
    # Linux aarch64
    plat_name = "manylinux_2_28_aarch64"


def read_requirements(*filepath):
    requirements = []
    with open(os.path.join(ROOT_DIR, *filepath)) as file:
        requirements = file.read().splitlines()
    return requirements


def complete_version_file(*filepath):
    today = date.today()
    dstr = today.strftime("%Y%m%d")
    with open(os.path.join(".", *filepath), "r") as fp:
        content = fp.read()

    content = content.replace("$$DATE$$", dstr)

    with open(os.path.join(".", *filepath), "w+") as fp:
        fp.write(content)


def find_version(*filepath):
    complete_version_file(*filepath)
    # Extract version information from filepath
    with open(os.path.join(".", *filepath)) as fp:
        version_match = re.search(
            r"^__version__ = ['\"]([^'\"]*)['\"]", fp.read(), re.M
        )
        if version_match:
            return version_match.group(1)
        print("Unable to find version string.")
        exit(-1)


setup(
    name="secretflow-dataproxy",
    version=find_version("dataproxy", "version.py"),
    author="SecretFlow Team",
    author_email="secretflow-contact@service.alipay.com",
    description="DataProxy SDK",
    long_description="",
    license="Apache 2.0",
    classifiers=[
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    packages=find_packages(),
    ext_modules=[BazelExtension("dataproxy")],
    cmdclass={"build_ext": BazelBuild},
    distclass=BinaryDistribution,
    python_requires=">=3.9, <3.12",
    install_requires=read_requirements("requirements.txt"),
    setup_requires=["wheel"],
    options={"bdist_wheel": {"plat_name": plat_name}},
)