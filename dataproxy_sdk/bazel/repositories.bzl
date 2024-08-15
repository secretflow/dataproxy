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

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

def dataproxy_deps():
    _bazel_platform()
    _bazel_rules_pkg()
    _rules_proto_grpc()

    _com_github_nelhage_rules_boost()
    _com_github_facebook_zstd()
    _org_sourceware_bzip2()
    _com_github_google_brotli()
    _com_github_lz4_lz4()
    _com_github_google_snappy()
    _com_google_double_conversion()
    _com_github_tencent_rapidjson()
    _com_github_xtensor_xsimd()
    _org_apache_thrift()
    _org_apache_orc()
    _org_apache_arrow()
    _com_github_pybind11_bazel()
    _com_github_pybind11()
    _com_github_grpc_grpc()

    _kuscia()
    _yacl()

def _yacl():
    maybe(
        http_archive,
        name = "yacl",
        urls = [
            "https://github.com/secretflow/yacl/archive/refs/tags/0.4.5b2.tar.gz",
        ],
        strip_prefix = "yacl-0.4.5b2",
        sha256 = "b3fb75d41a32b80145a3bb9d36b8c039a262191f1a2f037292c649344289b01b",
    )

def _kuscia():
    maybe(
        http_archive,
        name = "kuscia",
        urls = [
            "https://github.com/secretflow/kuscia/archive/refs/tags/v0.9.0b0.tar.gz",
        ],
        strip_prefix = "kuscia-0.9.0b0",
        sha256 = "851455f4a3ba70850c8a751a78ebfbbb9fd6d78ec902d0cbf32c2c565d1c8410",
    )

def _bazel_rules_pkg():
    http_archive(
        name = "rules_pkg",
        sha256 = "8f9ee2dc10c1ae514ee599a8b42ed99fa262b757058f65ad3c384289ff70c4b8",
        urls = [
            "https://github.com/bazelbuild/rules_pkg/releases/download/0.9.1/rules_pkg-0.9.1.tar.gz",
        ],
    )

def _bazel_platform():
    http_archive(
        name = "platforms",
        urls = [
            "https://mirror.bazel.build/github.com/bazelbuild/platforms/releases/download/0.0.8/platforms-0.0.8.tar.gz",
            "https://github.com/bazelbuild/platforms/releases/download/0.0.8/platforms-0.0.8.tar.gz",
        ],
        sha256 = "8150406605389ececb6da07cbcb509d5637a3ab9a24bc69b1101531367d89d74",
    )

def _org_sourceware_bzip2():
    maybe(
        http_archive,
        name = "bzip2",
        build_file = "@dataproxy//dataproxy_sdk/bazel:bzip2.BUILD",
        sha256 = "ab5a03176ee106d3f0fa90e381da478ddae405918153cca248e682cd0c4a2269",
        strip_prefix = "bzip2-1.0.8",
        urls = [
            "https://sourceware.org/pub/bzip2/bzip2-1.0.8.tar.gz",
        ],
    )

def _com_github_lz4_lz4():
    maybe(
        http_archive,
        name = "com_github_lz4_lz4",
        sha256 = "030644df4611007ff7dc962d981f390361e6c97a34e5cbc393ddfbe019ffe2c1",
        strip_prefix = "lz4-1.9.3",
        type = "tar.gz",
        build_file = "@dataproxy//dataproxy_sdk/bazel:lz4.BUILD",
        urls = [
            "https://codeload.github.com/lz4/lz4/tar.gz/refs/tags/v1.9.3",
        ],
    )

def _com_google_double_conversion():
    maybe(
        http_archive,
        name = "com_google_double_conversion",
        sha256 = "04ec44461850abbf33824da84978043b22554896b552c5fd11a9c5ae4b4d296e",
        strip_prefix = "double-conversion-3.3.0",
        build_file = "@dataproxy//dataproxy_sdk/bazel:double_conversion.BUILD",
        urls = [
            "https://github.com/google/double-conversion/archive/refs/tags/v3.3.0.tar.gz",
        ],
    )

def _com_github_xtensor_xsimd():
    maybe(
        http_archive,
        name = "com_github_xtensor_xsimd",
        sha256 = "d52551360d37709675237d2a0418e28f70995b5b7cdad7c674626bcfbbf48328",
        type = "tar.gz",
        strip_prefix = "xsimd-8.1.0",
        build_file = "@dataproxy//dataproxy_sdk/bazel:xsimd.BUILD",
        urls = [
            "https://codeload.github.com/xtensor-stack/xsimd/tar.gz/refs/tags/8.1.0",
        ],
    )

def _com_github_nelhage_rules_boost():
    # use boost 1.83
    RULES_BOOST_COMMIT = "cfa585b1b5843993b70aa52707266dc23b3282d0"
    maybe(
        http_archive,
        name = "com_github_nelhage_rules_boost",
        sha256 = "a7c42df432fae9db0587ff778d84f9dc46519d67a984eff8c79ae35e45f277c1",
        strip_prefix = "rules_boost-%s" % RULES_BOOST_COMMIT,
        patch_args = ["-p1"],
        patches = ["@dataproxy//dataproxy_sdk/bazel:patches/rules_boost.patch"],
        urls = [
            "https://github.com/nelhage/rules_boost/archive/%s.tar.gz" % RULES_BOOST_COMMIT,
        ],
    )

def _com_github_facebook_zstd():
    maybe(
        http_archive,
        name = "com_github_facebook_zstd",
        build_file = "@dataproxy//dataproxy_sdk/bazel:zstd.BUILD",
        strip_prefix = "zstd-1.5.6",
        sha256 = "8c29e06cf42aacc1eafc4077ae2ec6c6fcb96a626157e0593d5e82a34fd403c1",
        type = ".tar.gz",
        urls = [
            "https://github.com/facebook/zstd/releases/download/v1.5.6/zstd-1.5.6.tar.gz",
        ],
    )

def _com_github_google_snappy():
    maybe(
        http_archive,
        name = "com_github_google_snappy",
        sha256 = "75c1fbb3d618dd3a0483bff0e26d0a92b495bbe5059c8b4f1c962b478b6e06e7",
        strip_prefix = "snappy-1.1.9",
        build_file = "@dataproxy//dataproxy_sdk/bazel:snappy.BUILD",
        urls = [
            "https://github.com/google/snappy/archive/refs/tags/1.1.9.tar.gz",
        ],
    )

def _com_github_google_brotli():
    maybe(
        http_archive,
        name = "brotli",
        build_file = "@dataproxy//dataproxy_sdk/bazel:brotli.BUILD",
        sha256 = "e720a6ca29428b803f4ad165371771f5398faba397edf6778837a18599ea13ff",
        strip_prefix = "brotli-1.1.0",
        urls = [
            "https://github.com/google/brotli/archive/refs/tags/v1.1.0.tar.gz",
        ],
    )

def _com_github_tencent_rapidjson():
    maybe(
        http_archive,
        name = "com_github_tencent_rapidjson",
        sha256 = "bf7ced29704a1e696fbccf2a2b4ea068e7774fa37f6d7dd4039d0787f8bed98e",
        strip_prefix = "rapidjson-1.1.0",
        build_file = "@dataproxy//dataproxy_sdk/bazel:rapidjson.BUILD",
        urls = [
            "https://github.com/Tencent/rapidjson/archive/refs/tags/v1.1.0.tar.gz",
        ],
    )

def _org_apache_thrift():
    maybe(
        http_archive,
        name = "org_apache_thrift",
        build_file = "@dataproxy//dataproxy_sdk/bazel:thrift.BUILD",
        sha256 = "5da60088e60984f4f0801deeea628d193c33cec621e78c8a43a5d8c4055f7ad9",
        strip_prefix = "thrift-0.13.0",
        urls = [
            "https://github.com/apache/thrift/archive/v0.13.0.tar.gz",
        ],
    )

def _org_apache_arrow():
    maybe(
        http_archive,
        name = "org_apache_arrow",
        sha256 = "07cdb4da6795487c800526b2865c150ab7d80b8512a31793e6a7147c8ccd270f",
        strip_prefix = "arrow-apache-arrow-14.0.2",
        build_file = "@dataproxy//dataproxy_sdk/bazel:arrow.BUILD",
        urls = [
            "https://github.com/apache/arrow/archive/refs/tags/apache-arrow-14.0.2.tar.gz",
        ],
    )

def _org_apache_orc():
    maybe(
        http_archive,
        name = "org_apache_orc",
        sha256 = "3037fd324a17994f55146aae342531c4a343fdc1ac698c5c6f0f5b7a75ece501",
        strip_prefix = "orc-1.9.3",
        build_file = "@dataproxy//dataproxy_sdk/bazel:orc.BUILD",
        urls = [
            "https://github.com/apache/orc/archive/refs/tags/v1.9.3.tar.gz",
        ],
    )

def _com_github_pybind11_bazel():
    maybe(
        http_archive,
        name = "pybind11_bazel",
        sha256 = "2d3316d89b581966fc11eab9aa9320276baee95c8233c7a8efc7158623a48de0",
        strip_prefix = "pybind11_bazel-ff261d2e9190955d0830040b20ea59ab9dbe66c8",
        urls = [
            "https://github.com/pybind/pybind11_bazel/archive/ff261d2e9190955d0830040b20ea59ab9dbe66c8.zip",
        ],
    )

def _com_github_pybind11():
    maybe(
        http_archive,
        name = "pybind11",
        build_file = "@pybind11_bazel//:pybind11.BUILD",
        sha256 = "d475978da0cdc2d43b73f30910786759d593a9d8ee05b1b6846d1eb16c6d2e0c",
        strip_prefix = "pybind11-2.11.1",
        urls = [
            "https://github.com/pybind/pybind11/archive/refs/tags/v2.11.1.tar.gz",
        ],
    )

def _rules_proto_grpc():
    http_archive(
        name = "rules_proto_grpc",
        sha256 = "928e4205f701b7798ce32f3d2171c1918b363e9a600390a25c876f075f1efc0a",
        strip_prefix = "rules_proto_grpc-4.4.0",
        urls = [
            "https://github.com/rules-proto-grpc/rules_proto_grpc/releases/download/4.4.0/rules_proto_grpc-4.4.0.tar.gz",
        ],
    )

def _com_github_grpc_grpc():
    maybe(
        http_archive,
        name = "com_github_grpc_grpc",
        sha256 = "7f42363711eb483a0501239fd5522467b31d8fe98d70d7867c6ca7b52440d828",
        strip_prefix = "grpc-1.51.0",
        type = "tar.gz",
        patch_args = ["-p1"],
        patches = ["@dataproxy//dataproxy_sdk/bazel:patches/grpc.patch"],
        urls = [
            "https://github.com/grpc/grpc/archive/refs/tags/v1.51.0.tar.gz",
        ],
    )
