# Copyright 2025 Ant Group Co., Ltd.
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

_PYTHON_VERSION_FLAG = Label("@rules_python//python/config_settings:python_version_major_minor")

def _py_versions_select(name, version_map, default):
    select_map = {}
    for version, value in version_map.items():
        # 为每个版本生成唯一的config_setting名称
        config_name = "{name}_py_{version}".format(name = name, version = version)
        native.config_setting(
            name = config_name,
            flag_values = {
                _PYTHON_VERSION_FLAG: version,
            },
        )
        select_map[":%s" % config_name] = value
    select_map["//conditions:default"] = default
    return select(select_map)

def py_versions_select(name, version_map, default = "none"):
    """Returns the value for the given version_map based on the current Python version.

    Args:
        name: The name of the target.
        version_map: A map from python version to value to return if the current python
            version matches that version. If no versions match, then default is returned.
        default: The default value to return if none of the versions in the version_map match
            the current python version. Defaults to "none".

    Returns:
        The value corresponding to the current python version.
    """
    return _py_versions_select(name, version_map, default)
