// Copyright 2024 Ant Group Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "yacl/base/exception.h"

namespace dataproxy_sdk {

#define DATAPROXY_THROW(...) YACL_THROW_WITH_STACK(__VA_ARGS__)

#define DATAPROXY_ENFORCE(...) YACL_ENFORCE(__VA_ARGS__)

#define DATAPROXY_ENFORCE_EQ(...) YACL_ENFORCE_EQ(__VA_ARGS__)

#define CHECK_ARROW_OR_THROW(statement)        \
  do {                                         \
    auto __s__ = (statement);                  \
    if (!__s__.ok()) {                         \
      DATAPROXY_THROW("{}", __s__.ToString()); \
    }                                          \
  } while (false)

#define CHECK_RESP_OR_THROW(resp)             \
  do {                                        \
    auto __s__ = (resp).status();             \
    if (__s__.code()) {                       \
      DATAPROXY_THROW("{}", __s__.message()); \
    }                                         \
  } while (false)

// For StatusOr from Asylo.
#define ASSIGN_ARROW_OR_THROW(lhs, rexpr)               \
  do {                                                  \
    auto __s__ = (rexpr);                               \
    if (!__s__.ok()) {                                  \
      DATAPROXY_THROW("{}", __s__.status().ToString()); \
    }                                                   \
    lhs = std::move(__s__).ValueOrDie();                \
  } while (false)

#define ASSIGN_DP_OR_THROW(lhs, rexpr)                              \
  auto&& _error_or_value = (rexpr);                                 \
  do {                                                              \
    if ((__builtin_expect(!!(!(_error_or_value).ok()), 0))) {       \
      DATAPROXY_THROW("{}", (_error_or_value).status().ToString()); \
    }                                                               \
  } while (0);                                                      \
  lhs = std::move(_error_or_value).ValueUnsafe();

}  // namespace dataproxy_sdk