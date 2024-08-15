/*
 * Copyright 2023 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.secretflow.dataproxy.manager;

import java.util.List;

/**
 * Dataset reader
 *
 * @author muhong
 * @date 2023-08-21 17:48
 */
public interface DataReader {

    /**
     * Build split dataset reader
     *
     * @param splitNumber Split number
     * @return Split reader list
     */
    List<SplitReader> createSplitReader(int splitNumber);
}
