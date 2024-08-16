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

load("@rules_proto_grpc//cpp:defs.bzl", "cpp_grpc_compile")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # Apache 2.0

exports_files(["LICENSE.txt"])

genrule(
    name = "Adaptor_h",
    srcs = ["c++/src/Adaptor.hh.in"],
    outs = ["c++/src/Adaptor.hh"],
    cmd = ("sed " +
           "-e 's/cmakedefine HAS_PREAD/define HAS_PREAD/g' " +
           "-e 's/cmakedefine HAS_STRPTIME/define HAS_STRPTIME/g' " +
           "-e 's/cmakedefine HAS_DIAGNOSTIC_PUSH/define HAS_DIAGNOSTIC_PUSH/g' " +
           "-e 's/cmakedefine HAS_DOUBLE_TO_STRING/define HAS_DOUBLE_TO_STRING/g' " +
           "-e 's/cmakedefine HAS_INT64_TO_STRING/define HAS_INT64_TO_STRING/g' " +
           "-e 's/cmakedefine HAS_PRE_1970/define HAS_PRE_1970/g' " +
           "-e 's/cmakedefine HAS_POST_2038/define HAS_POST_2038/g' " +
           "-e 's/cmakedefine HAS_STD_ISNAN/define HAS_STD_ISNAN/g' " +
           "-e 's/cmakedefine HAS_BUILTIN_OVERFLOW_CHECK/define HAS_BUILTIN_OVERFLOW_CHECK/g' " +
           "-e 's/cmakedefine NEEDS_Z_PREFIX/undef NEEDS_Z_PREFIX/g' " +
           "$< >$@"),
)

genrule(
    name = "orc-config",
    srcs = ["c++/include/orc/orc-config.hh.in"],
    outs = ["c++/include/orc/orc-config.hh"],
    cmd = ("sed " +
           "-e 's/@ORC_VERSION@/1.9.0/g' " +
           "-e 's/cmakedefine ORC_CXX_HAS_CSTDINT/undef ORC_CXX_HAS_CSTDINT/g' " +
           "$< >$@"),
)

proto_library(
    name = "orc_proto",
    srcs = ["proto/orc_proto.proto"],
    strip_import_prefix = "proto",
)

cpp_grpc_compile(
    name = "orc_proto_cc",
    prefix_path = "../c++",
    protos = [":orc_proto"],
)

filegroup(
    name = "orc_proto_cc_files",
    srcs = [
        ":orc_proto_cc",
    ],
)

genrule(
    name = "orc_proto_cc_file_copy",
    srcs = [":orc_proto_cc_files"],
    outs = [
        "c++/src/orc_proto.pb.cc",
        "c++/src/orc_proto.pb.h",
    ],
    cmd = "cp $(locations :orc_proto_cc_files) $(@D)/c++/src",
)

cc_library(
    name = "orc",
    srcs = glob(
        [
            "c++/src/*.cc",
            "c++/src/*.hh",
            "c++/src/sargs/*.cc",
            "c++/src/sargs/*.hh",
            "c++/src/io/*.cc",
            "c++/src/io/*.hh",
            "c++/src/wrap/*.cc",
            "c++/src/wrap/*.hh",
            "c++/src/wrap/*.h",
        ],
        exclude = [
            "c++/src/OrcHdfsFile.cc",
            "c++/src/BpackingAvx512.cc",
        ],
    ) + [
        "c++/src/Adaptor.hh",
        "c++/src/orc_proto.pb.cc",
    ],
    hdrs = glob([
        "c++/include/orc/*.hh",
        "c++/include/orc/**/*.hh",
    ]) + [
        "c++/include/orc/orc-config.hh",
        "c++/src/orc_proto.pb.cc",
        "c++/src/orc_proto.pb.h",
    ],
    includes = [
        "c++/include",
        "c++/src",
    ],
    deps = [
        "@com_github_facebook_zstd//:zstd",
        "@com_github_google_snappy//:snappy",
        "@com_github_lz4_lz4//:lz4",
        "@com_google_protobuf//:protobuf",
        "@zlib",
    ],
)
