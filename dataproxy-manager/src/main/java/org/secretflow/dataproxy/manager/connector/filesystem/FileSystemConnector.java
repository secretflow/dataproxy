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

package org.secretflow.dataproxy.manager.connector.filesystem;

import org.secretflow.dataproxy.common.exceptions.DataproxyErrorCode;
import org.secretflow.dataproxy.common.exceptions.DataproxyException;
import org.secretflow.dataproxy.common.model.FlightContentFormatConfig;
import org.secretflow.dataproxy.common.model.FlightContentFormatTypeEnum;
import org.secretflow.dataproxy.common.model.InferSchemaResult;
import org.secretflow.dataproxy.common.model.command.DatasetReadCommand;
import org.secretflow.dataproxy.common.model.command.DatasetWriteCommand;
import org.secretflow.dataproxy.common.model.dataset.DatasetFormatConfig;
import org.secretflow.dataproxy.common.model.dataset.format.CSVFormatConfig;
import org.secretflow.dataproxy.common.model.datasource.DatasourceTypeEnum;
import org.secretflow.dataproxy.common.model.datasource.conn.ConnConfig;
import org.secretflow.dataproxy.common.model.datasource.conn.LocalFileSystemConnConfig;
import org.secretflow.dataproxy.common.model.datasource.conn.ObjectFileSystemConnConfig;
import org.secretflow.dataproxy.common.model.datasource.location.FileSystemLocationConfig;
import org.secretflow.dataproxy.common.model.datasource.location.LocationConfig;
import org.secretflow.dataproxy.common.utils.DPStringUtils;
import org.secretflow.dataproxy.common.utils.JsonUtils;
import org.secretflow.dataproxy.manager.Connector;
import org.secretflow.dataproxy.manager.DataReader;
import org.secretflow.dataproxy.manager.DataWriter;
import org.secretflow.dataproxy.manager.SplitReader;

import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

/**
 * File system connector
 *
 * @author muhong
 * @date 2023-09-11 09:49
 */
@Slf4j
public class FileSystemConnector implements Connector {

    /**
     * Root uri
     */
    private final String rootUri;

    /**
     * Filesystem
     */
    private final FileSystem fileSystem;

    /**
     * 文件系统连接器构造函数
     *
     * @param type       文件系统数据源类型
     * @param connConfig 文件系统数据源连接信息，根据类型不同而不同
     */
    public FileSystemConnector(DatasourceTypeEnum type, ConnConfig connConfig) {
        // 文件系统参数构建
        Configuration configuration = new Configuration();
        switch (type) {
            case MINIO: {
                ObjectFileSystemConnConfig minioConnConfig = (ObjectFileSystemConnConfig) connConfig;
                rootUri = generateUri(type.getScheme(), minioConnConfig.getBucket(), minioConnConfig.getObjectKeyPrefix());
                configuration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
                configuration.set("fs.s3a.endpoint", minioConnConfig.getEndpoint());
                configuration.set("fs.s3a.access.key", minioConnConfig.getAccessKey());
                configuration.set("fs.s3a.secret.key", minioConnConfig.getAccessSecret());
                configuration.set("fs.s3a.buffer.dir", "./dp/buffer");
                configuration.set("fs.s3a.connection.ssl.enabled", "false");
                configuration.setInt("fs.s3a.connection.timeout", 7200000);
                // 减少重试次数，避免阻塞
                configuration.setInt("fs.s3a.attempts.maximum", 1);
                configuration.setInt("fs.s3a.retry.limit", 1);
                break;
            }
            case OSS: {
                ObjectFileSystemConnConfig ossConnConfig = (ObjectFileSystemConnConfig) connConfig;
                rootUri = generateUri(type.getScheme(), ossConnConfig.getBucket(), ossConnConfig.getObjectKeyPrefix());
                configuration.set("fs.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem");
                configuration.set("fs.oss.endpoint", ossConnConfig.getEndpoint());
                configuration.set("fs.oss.accessKeyId", ossConnConfig.getAccessKey());
                configuration.set("fs.oss.accessKeySecret", ossConnConfig.getAccessSecret());
                configuration.set("fs.oss.buffer.dir", "./dp/buffer");
                configuration.set("fs.oss.timeout.millisecond", String.valueOf(7200000));
                configuration.setInt("fs.oss.attempts.maximum", 1);
                break;
            }
            case OBS: {
                ObjectFileSystemConnConfig obsConnConfig = (ObjectFileSystemConnConfig) connConfig;
                rootUri = generateUri(type.getScheme(), obsConnConfig.getBucket(), obsConnConfig.getObjectKeyPrefix());
                configuration.set("fs.obs.impl", "org.apache.hadoop.fs.obs.OBSFileSystem");
                configuration.set("fs.obs.endpoint", obsConnConfig.getEndpoint());
                configuration.set("fs.obs.accessKeyId", obsConnConfig.getAccessKey());
                configuration.set("fs.obs.accessKeySecret", obsConnConfig.getAccessSecret());
                configuration.set("fs.obs.buffer.dir", "./dp/buffer");
                configuration.set("fs.obs.timeout.millisecond", String.valueOf(7200000));
                configuration.setInt("fs.obs.attempts.maximum", 1);
                break;
            }
            case LOCAL_HOST: {
                LocalFileSystemConnConfig localFsConnConfig = (LocalFileSystemConnConfig) connConfig;
                rootUri = generateUri(type.getScheme(), localFsConnConfig.getPath());
                break;
            }
            default:
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "不支持的文件系统数据源" + type);
        }

        // 构建文件系统连接器
        try {
            this.fileSystem = FileSystem.newInstance(new URI(rootUri), configuration);
        } catch (Exception e) {
            log.error("[FileSystemConnector] 创建文件系统连接器失败，type:{}, config:{}", type, JsonUtils.toJSONString(connConfig), e);
            throw DataproxyException.of(DataproxyErrorCode.CREATE_DATASOURCE_CONNECTOR_ERROR, e);
        }
    }

    @Override
    public InferSchemaResult inferSchema(BufferAllocator allocator, LocationConfig locationConfig, DatasetFormatConfig formatConfig) {
        String uri = generateFileUri(((FileSystemLocationConfig) locationConfig).getRelativePath());

        DataReader dataReader = null;
        switch (formatConfig.getType()) {
            case CSV:
                dataReader = new CSVDataReader(allocator, this.fileSystem, uri, null, (CSVFormatConfig) formatConfig.getFormatConfig(), null);
                break;
            case BINARY_FILE:
                dataReader = new BinaryFileDataReader(allocator, this.fileSystem, uri);
                break;
            default:
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "不支持的文件格式 " + formatConfig.getType());
        }
        List<SplitReader> splitReader = dataReader.createSplitReader(1);
        try (ArrowReader arrowReader = splitReader.get(0).startRead()) {
            return InferSchemaResult.builder()
                .schema(arrowReader.getVectorSchemaRoot().getSchema())
                .datasetFormatConfig(DatasetFormatConfig.builder()
                    .type(formatConfig.getType())
                    .formatConfig(formatConfig.getFormatConfig())
                    .build())
                .build();
        } catch (Exception e) {
            throw DataproxyException.of(DataproxyErrorCode.READER_RELEASE_FAILED, e);
        }
    }

    @Override
    public DataReader buildReader(BufferAllocator allocator, DatasetReadCommand readCommand) {
        FileSystemLocationConfig fileSystemLocationConfig = (FileSystemLocationConfig) readCommand.getLocationConfig().getLocationConfig();
        String uri = generateFileUri(fileSystemLocationConfig.getRelativePath());

        FlightContentFormatConfig outputFormatConfig = readCommand.getOutputFormatConfig();
        switch (readCommand.getFormatConfig().getType()) {
            case CSV:
                // 存储格式为csv，输出指定为结构化数据（或缺省）时，结构化输出，其余都按二进制输出
                if (outputFormatConfig == null || outputFormatConfig.getFormatType() == FlightContentFormatTypeEnum.STRUCTURED_DATA) {
                    log.info("[FileSystemConnector - buildReader] 结构化数据读取，uri:{}", uri);
                    return new CSVDataReader(allocator, this.fileSystem, uri, readCommand.getSchema(), (CSVFormatConfig) readCommand.getFormatConfig().getFormatConfig(), readCommand.getFieldList());
                }
            case BINARY_FILE:
                log.info("[FileSystemConnector - buildReader] 二进制文件读取，uri:{}", uri);
                return new BinaryFileDataReader(allocator, this.fileSystem, uri);
            default:
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "不支持的文件格式 " + readCommand.getFormatConfig().getType());
        }
    }

    @Override
    public DataWriter buildWriter(DatasetWriteCommand writeCommand) {
        FileSystemLocationConfig fileSystemLocationConfig = (FileSystemLocationConfig) writeCommand.getLocationConfig().getLocationConfig();
        String uri = generateFileUri(fileSystemLocationConfig.getRelativePath());

        FlightContentFormatConfig inputFormatConfig = writeCommand.getInputFormatConfig();
        switch (writeCommand.getFormatConfig().getType()) {
            case CSV:
                if (inputFormatConfig == null || inputFormatConfig.getFormatType() == FlightContentFormatTypeEnum.STRUCTURED_DATA) {
                    log.info("[FileSystemConnector - buildWriter] STRUCTURED_DATA，uri:{}", uri);
                    return new CSVDataWriter(this.fileSystem, uri, (CSVFormatConfig) writeCommand.getFormatConfig().getFormatConfig());
                }
            case BINARY_FILE:
                log.info("[FileSystemConnector - buildWriter] BINARY_FILE，uri:{}", uri);
                return new BinaryFileDataWriter(this.fileSystem, uri);
            default:
                throw DataproxyException.of(DataproxyErrorCode.PARAMS_UNRELIABLE, "不支持的文件格式 " + writeCommand.getFormatConfig().getType());
        }
    }

    @Override
    public boolean isAvailable() {
        try {
            this.fileSystem.getFileStatus(new Path(rootUri));
            return true;
        } catch (Exception e) {
            log.info("[FileSystemConnector] check status error, uri:{}", this.rootUri, e);
            return false;
        }
    }

    @Override
    public void close() throws Exception {
        this.fileSystem.close();
    }

    /**
     * 生成文件路径
     *
     * @param scheme 协议类型
     * @param path
     * @return
     */
    private String generateUri(String scheme, String... path) {
        return scheme +
            DPStringUtils.joinWithoutEmpty("/",
                Arrays.stream(path).map(item -> DPStringUtils.removeDecorateIdentifier(item, "/")).toArray(String[]::new)
            );
    }

    private String generateFileUri(String relativePath){
        return DPStringUtils.removeDecorateIdentifier(this.rootUri, "/") + "/" + DPStringUtils.removeDecorateIdentifier(relativePath, "/");
    }
}
