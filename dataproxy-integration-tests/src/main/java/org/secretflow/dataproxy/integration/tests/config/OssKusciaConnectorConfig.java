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

package org.secretflow.dataproxy.integration.tests.config;

import org.secretflow.dataproxy.integration.tests.KusciaConnectorConfig;

import org.secretflow.v1alpha1.common.Common;
import org.secretflow.v1alpha1.kusciaapi.Domaindata;
import org.secretflow.v1alpha1.kusciaapi.Domaindatasource;

/**
 * @author muhong
 * @date 2023-11-17 11:08
 */
public class OssKusciaConnectorConfig implements KusciaConnectorConfig {

    private final static String OSS_HOST = "";
    private final static String OSS_PORT = "";
    private final static String OSS_BUCKET = "";
    private final static String OSS_ACCESS_KEY_ID = "";
    private final static String OSS_ACCESS_KEY_SECRET = "";
    private final static String OSS_STORAGE_TYPE = "minio";

    private final static String TEST_DATASOURCE_ID = "localhost_integration_test_datasource";
    private final static String TEST_DATASET_ID = "localhost_integration_test_dataset";
    private final static String TEST_OWNER = "integration_test_user";
    private final static String TEST_TABLE = "test/oss_all_types_write";

    @Override
    public Domaindatasource.DomainDataSource getDatasource() {
        return Domaindatasource.DomainDataSource.newBuilder()
            .setDatasourceId(TEST_DATASOURCE_ID)
            .setName(TEST_DATASOURCE_ID)
            .setType("oss")
            .setStatus("Available")
            .setInfo(Domaindatasource.DataSourceInfo.newBuilder()
                .setOss(Domaindatasource.OssDataSourceInfo.newBuilder()
                    .setEndpoint(OSS_HOST + ":" + OSS_PORT)
                    .setBucket(OSS_BUCKET)
                    .setAccessKeyId(OSS_ACCESS_KEY_ID)
                    .setAccessKeySecret(OSS_ACCESS_KEY_SECRET)
                    .setStorageType(OSS_STORAGE_TYPE)
                    .build())
                .build())
            .build();
    }

    @Override
    public Domaindata.DomainData getDataset() {
        return Domaindata.DomainData.newBuilder()
            .setDomaindataId(TEST_DATASET_ID)
            .setName(TEST_DATASET_ID)
            .setType("table")
            .setRelativeUri(TEST_TABLE)
            .setDatasourceId(TEST_DATASOURCE_ID)
            .setFileFormat(Common.FileFormat.CSV)
            .setVendor(TEST_OWNER)
            .build();
    }
}
