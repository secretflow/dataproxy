/*
 * Copyright 2025 Ant Group Co., Ltd.
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

package org.secretflow.dataproxy.plugin.database.reader;

import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.secretflow.dataproxy.plugin.database.utils.Record;
import org.secretflow.dataproxy.core.converter.*;
import org.secretflow.dataproxy.core.reader.AbstractSender;
import org.secretflow.dataproxy.core.visitor.*;

import javax.annotation.Nonnull;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public class DatabaseRecordSender extends AbstractSender<Record> {
    private final static Map<ArrowType.ArrowTypeID, ValueConversionStrategy> ARROW_TYPE_ID_FIELD_CONSUMER_MAP = new HashMap<>();
    private final Map<String, FieldVector> fieldVectorMap = new HashMap<>();

    private boolean isInit = false;
    private final String tableName;

    private final DatabaseMetaData metaData;
    static {
        SmallIntVectorConverter smallIntVectorConverter = new SmallIntVectorConverter(new ShortValueVisitor(), null);
        TinyIntVectorConverter tinyIntVectorConverter = new TinyIntVectorConverter(new ByteValueVisitor(), smallIntVectorConverter);
        BigIntVectorConverter bigIntVectorConverter = new BigIntVectorConverter(new LongValueVisitor(), tinyIntVectorConverter);
        IntVectorConverter intVectorConverter = new IntVectorConverter(new IntegerValueVisitor(), bigIntVectorConverter);
        Float4VectorConverter float4VectorConverter = new Float4VectorConverter(new FloatValueVisitor(), null);
        Float8VectorConverter float8VectorConverter = new Float8VectorConverter(new DoubleValueVisitor(), float4VectorConverter);
        DateMilliVectorConverter dateMilliVectorConverter = new DateMilliVectorConverter(new LongValueVisitor(), null);

        ARROW_TYPE_ID_FIELD_CONSUMER_MAP.put(ArrowType.ArrowTypeID.Int, intVectorConverter);
        ARROW_TYPE_ID_FIELD_CONSUMER_MAP.put(ArrowType.ArrowTypeID.Utf8, new VarCharVectorConverter(new ByteArrayValueVisitor()));
        ARROW_TYPE_ID_FIELD_CONSUMER_MAP.put(ArrowType.ArrowTypeID.FloatingPoint, float8VectorConverter);
        ARROW_TYPE_ID_FIELD_CONSUMER_MAP.put(ArrowType.ArrowTypeID.Bool, new BitVectorConverter(new BooleanValueVisitor()));
        ARROW_TYPE_ID_FIELD_CONSUMER_MAP.put(ArrowType.ArrowTypeID.Date, new DateDayVectorConverter(new IntegerValueVisitor(), dateMilliVectorConverter));
        ARROW_TYPE_ID_FIELD_CONSUMER_MAP.put(ArrowType.ArrowTypeID.Time, new TimeMilliVectorConvertor(new IntegerValueVisitor(), null));
        ARROW_TYPE_ID_FIELD_CONSUMER_MAP.put(ArrowType.ArrowTypeID.Timestamp, new TimeStampNanoVectorConverter(new LongValueVisitor()));
    }
    /**
     * Constructor
     *
     * @param estimatedRecordCount Estimated number of records to be sent
     * @param recordQueue          Queue, used to store records to be sent
     * @param root                 Arrow vector schema root
     */
    public DatabaseRecordSender(int estimatedRecordCount, LinkedBlockingQueue<Record> recordQueue, VectorSchemaRoot root, String tableName, DatabaseMetaData metaData, ResultSet resultSet) {
        super(estimatedRecordCount, recordQueue, root);

        this.tableName = tableName;
        this.metaData = metaData;
    }

    @Override
    protected void toArrowVector(Record record, @Nonnull VectorSchemaRoot root, int takeRecordCount) {
        log.trace("record: {}, takeRecordCount: {}", record, takeRecordCount);
        try {
            this.initRecordColumn2FieldMap(metaData, tableName);
            Optional<FieldVector> filedVectorOpt;
            FieldVector vector;
            ArrowType.ArrowTypeID arrowTypeID;

            Object recordColumnValue;

            ResultSet columns = metaData.getColumns(null, null, tableName, null);

            while (columns.next()) {
                String name = columns.getString("COLUMN_NAME");

                filedVectorOpt = Optional.ofNullable(this.fieldVectorMap.get(name));

                if (filedVectorOpt.isPresent()) {
                    vector = filedVectorOpt.get();
                    recordColumnValue = record.get(name);
                    arrowTypeID = vector.getField().getType().getTypeID();
                    if (Objects.isNull(recordColumnValue)) {
                        vector.setNull(takeRecordCount);
                        continue;
                    }
                    ValueConversionStrategy converter = ARROW_TYPE_ID_FIELD_CONSUMER_MAP.get(arrowTypeID);
                    if (converter != null) {
                        converter.convertAndSet(vector, takeRecordCount, recordColumnValue);
                    } else {
                        log.warn("No converter found for ArrowTypeID: {} (column: {})", arrowTypeID, name);
                    }

                }
            }
            columns.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected boolean isOver(Record record) {
        return record.isLastLine();
    }

    @Override
    public void putOver() throws InterruptedException {
        this.put(new Record());
    }

    public boolean equalsIgnoreCase(String s1, String s2) {
        return s1 == null ? s2 == null : s1.equalsIgnoreCase(s2);
    }

    private synchronized void initRecordColumn2FieldMap(DatabaseMetaData metaData, String tableName) throws SQLException {
        if (isInit) {
            return;
        }

        VectorSchemaRoot root = getRoot();

        if (Objects.isNull(root)) {
            return;
        }
        List<FieldVector> fieldVectors = root.getFieldVectors();

        ResultSet columns = metaData.getColumns(null, null, tableName, null);

        Optional<FieldVector> first;

        while (columns.next()) {
            String name = columns.getString("COLUMN_NAME");

            first = fieldVectors.stream()
                    .filter(fieldVector -> equalsIgnoreCase(fieldVector.getName(), name))
                    .findFirst();
            if (first.isPresent()) {
                fieldVectorMap.put(name, first.get());
            } else {
                log.debug("columnName: {} not in fieldVectors", name);
            }
        }
        columns.close();
        isInit = true;
    }
}
