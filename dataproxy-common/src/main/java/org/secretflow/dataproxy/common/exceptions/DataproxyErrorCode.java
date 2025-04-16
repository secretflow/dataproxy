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

package org.secretflow.dataproxy.common.exceptions;

import lombok.Getter;

/**
 * dataproxy error code enums
 *
 * @author muhong
 * @date 2023-09-14 14:32
 */
@Getter
public enum DataproxyErrorCode {

    SUCCESS(ErrorLevels.INFO, ErrorTypes.BIZ, "000", "success"),

    //============================= 系统错误【001-399】==================================
    // saas场景【001-99】
    SAAS_GET_FLIGHT_INFO_READ_ERROR(ErrorLevels.ERROR, ErrorTypes.SYSTEM, "001", "Exception handling request to get data read from the terminal"),
    SAAS_GET_FLIGHT_INFO_WRITE_ERROR(ErrorLevels.ERROR, ErrorTypes.SYSTEM, "002", "Exception handling request to get data written to the terminal"),
    // kuscia场景【100-199】
    KUSCIA_GET_FLIGHT_INFO_QUERY_ERROR(ErrorLevels.ERROR, ErrorTypes.SYSTEM, "101", "Exception handling request to get data read from the terminal"),
    KUSCIA_GET_FLIGHT_INFO_UPDATE_ERROR(ErrorLevels.ERROR, ErrorTypes.SYSTEM, "102", "Exception handling request to get data written to the terminal"),
    KUSCIA_GET_STREAM_ERROR(ErrorLevels.ERROR, ErrorTypes.SYSTEM, "103", "Data read error"),
    KUSCIA_ACCEPT_PUT_ERROR(ErrorLevels.ERROR, ErrorTypes.SYSTEM, "104", "Data write error"),

    // common 200-399
    DATASET_WRITE_ERROR(ErrorLevels.ERROR, ErrorTypes.SYSTEM, "200", "Data Write Exception"),
    CACHE_SERVICE_BEAN_NOT_FOUND(ErrorLevels.ERROR, ErrorTypes.BIZ, "500", "Cannot find Cache Service Bean"),


    //============================= 业务错误【400-850】==================================
    // 通用参数异常400-409
    PARAMS_NOT_EXIST_ERROR(ErrorLevels.ERROR, ErrorTypes.PARAM, "400", "Exception of missing parameter"),
    PARAMS_UNRELIABLE(ErrorLevels.ERROR, ErrorTypes.PARAM, "401", "Invalid parameter"),
    // 框架异常410-449
    TICKET_UNAVAILABLE(ErrorLevels.ERROR, ErrorTypes.BIZ, "410", "Ticket invalid or expired"),
    CREATE_DATASOURCE_CONNECTOR_ERROR(ErrorLevels.ERROR, ErrorTypes.BIZ, "411", "Failed to create data source connector"),
    UNSUPPORTED_FIELD_TYPE(ErrorLevels.ERROR, ErrorTypes.PARAM, "412", "Unsupported field type"),
    INVALID_PARTITION_SPEC(ErrorLevels.ERROR, ErrorTypes.BIZ, "413", "Invalid partition expression"),

    // jdbc数据源类450-499
    JDBC_DATASOURCE_CONNECTION_POOL_BUILD_ERROR(ErrorLevels.ERROR, ErrorTypes.BIZ, "450", "Failed to create JDBC connection pool"),
    JDBC_DATASOURCE_CONNECTION_VALIDATE_FAILED(ErrorLevels.ERROR, ErrorTypes.PARAM, "451", "JDBC data source connectivity test failed"),
    JDBC_CALL_ERROR(ErrorLevels.ERROR, ErrorTypes.BIZ, "452", "JDBC data source request failed"),
    JDBC_GET_PRIMARY_KEY_FAILED(ErrorLevels.ERROR, ErrorTypes.BIZ, "453", "Failed to infer primary key"),
    JDBC_GET_PARTITION_STATS_FAILED(ErrorLevels.ERROR, ErrorTypes.BIZ, "454", "Failed to infer pagination parameters"),
    JDBC_FETCH_BATCH_DATA_FAILED(ErrorLevels.ERROR, ErrorTypes.BIZ, "455", "Failed to retrieve data block"),
    JDBC_GET_CONN_THREAD_FAILED(ErrorLevels.ERROR, ErrorTypes.BIZ, "456", "Failed to get connection from pool"),
    JDBC_CREATE_TABLE_FAILED(ErrorLevels.ERROR, ErrorTypes.BIZ, "460", "Failed to create table"),
    JDBC_INSERT_INTO_TABLE_FAILED(ErrorLevels.ERROR, ErrorTypes.BIZ, "461", "Failed to write data into table"),
    UNSUPPORTED_INDEX_TYPE(ErrorLevels.ERROR, ErrorTypes.PARAM, "462", "Unsupported index type"),
    FIELD_NOT_EXIST(ErrorLevels.ERROR, ErrorTypes.PARAM, "463", "Field does not exist"),

    // 文件类数据源500-549
    FILE_READ_STREAM_CREATE_FAILED(ErrorLevels.ERROR, ErrorTypes.BIZ, "500", "Failed to create file read stream"),
    FILE_BATCH_DOWNLOAD_FAILED(ErrorLevels.ERROR, ErrorTypes.BIZ, "501", "Failed to download file data block"),
    GET_FILE_SIZE_FAILED(ErrorLevels.ERROR, ErrorTypes.BIZ, "502", "Failed to retrieve file size"),
    FILE_WRITE_STREAM_CREATE_FAILED(ErrorLevels.ERROR, ErrorTypes.BIZ, "503", "Failed to create a file output stream for writing"),
    BINARY_DATA_FIELD_NOT_EXIST(ErrorLevels.ERROR, ErrorTypes.PARAM, "503", "The 'binary_data' column does not exist in the binary file for writing"),
    READ_DATA_LINE_FAILED(ErrorLevels.ERROR, ErrorTypes.BIZ, "504", "Failed to read data row"),
    HEADER_LINE_NOT_EXIST(ErrorLevels.ERROR, ErrorTypes.PARAM, "505", "Original file table header does not exist"),
    HEADER_LINE_PARSE_FAILED(ErrorLevels.ERROR, ErrorTypes.PARAM, "506", "Table header parsing failed"),
    VALUE_LINE_PARSE_FAILED(ErrorLevels.ERROR, ErrorTypes.PARAM, "507", "Failed to parse data row"),
    BOM_REMOVE_FAILED(ErrorLevels.ERROR, ErrorTypes.BIZ, "508", "Failed to remove BOM header"),
    DETECT_ENCODING_FAILED(ErrorLevels.ERROR, ErrorTypes.PARAM, "509", "Failed to infer CSV file encoding format"),
    READER_RELEASE_FAILED(ErrorLevels.ERROR, ErrorTypes.BIZ, "510", "Failed to release data read stream"),
    DATA_FORMAT_CONVERT_FAILED(ErrorLevels.ERROR, ErrorTypes.BIZ, "511", "Failed to convert target data format"),

    // odps 异常
    ODPS_CREATE_TABLE_FAILED(ErrorLevels.ERROR, ErrorTypes.BIZ, "600", "Create ODPS table failed"),
    ODPS_CREATE_PARTITION_FAILED(ErrorLevels.ERROR, ErrorTypes.BIZ, "601", "Create ODPS table failed"),
    ODPS_ERROR(ErrorLevels.ERROR, ErrorTypes.BIZ, "602", "ODPS error"),
    ODPS_TABLE_ALREADY_EXISTS(ErrorLevels.ERROR, ErrorTypes.BIZ, "603", "odps table already exists"),
    ODPS_TABLE_NOT_EXISTS(ErrorLevels.ERROR, ErrorTypes.BIZ, "604", "odps table not exists"),
    ODPS_PARTITION_ALREADY_EXISTS(ErrorLevels.ERROR, ErrorTypes.BIZ, "605", "odps partition already exists"),
    ODPS_PARTITION_NOT_EXISTS(ErrorLevels.ERROR, ErrorTypes.BIZ, "606", "odps partition not exists"),
    ODPS_TABLE_NOT_EMPTY(ErrorLevels.ERROR, ErrorTypes.BIZ, "607", "odps table not empty"),
    ODPS_TABLE_NOT_SUPPORT_PARTITION(ErrorLevels.ERROR, ErrorTypes.BIZ, "608", "odps table not support partition"),
    ODPS_TASK_NOT_READY(ErrorLevels.ERROR, ErrorTypes.BIZ, "609", "odps task not ready"),
    ODPS_TASK_NOT_RUN(ErrorLevels.ERROR, ErrorTypes.BIZ, "610", "odps task not run"),


    //============================= 第三方错误【900-999】==================================

    ;
    /**
     * Error code prefix (2 character)
     */
    private final static String ERROR_PREFIX = "DP";

    /**
     * Error version (1 character)
     */
    private final static String ERROR_VERSION = "0";

    /**
     * Error scene (4 character)
     * 0001 dataproxy
     */
    private final static String ERROR_SCENE = "0001";

    /**
     * Error level (1 character)
     */
    private final ErrorLevels errorLevel;

    /**
     * Error type (1 character)
     */
    private final ErrorTypes errorType;

    /**
     * Error specific id (3 character)
     */
    private final String errorSpecific;

    /**
     * Error code (12 character): {@link #ERROR_PREFIX} + {@link #ERROR_VERSION} + {@link #ERROR_SCENE} + {@link #errorLevel} + {@link #errorType} + {@link #errorSpecific}
     */
    private final String errorCode;

    /**
     * Error message
     */
    private final String errorMessage;

    /**
     * Construct function
     *
     * @param errorLevel    error level
     * @param errorType     error type
     * @param errorSpecific error specific id
     * @param errorMessage  error message
     */
    DataproxyErrorCode(ErrorLevels errorLevel,
                       ErrorTypes errorType,
                       String errorSpecific,
                       String errorMessage) {
        this.errorLevel = errorLevel;
        this.errorType = errorType;
        this.errorSpecific = errorSpecific;
        this.errorMessage = errorMessage;
        this.errorCode = ERROR_PREFIX + ERROR_VERSION + ERROR_SCENE + errorLevel.getCode()
            + errorType.getCode() + errorSpecific;
    }
}

