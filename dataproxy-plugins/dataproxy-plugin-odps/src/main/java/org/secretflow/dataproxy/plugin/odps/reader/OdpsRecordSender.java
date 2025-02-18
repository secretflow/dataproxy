/*
 * Copyright 2024 Ant Group Co., Ltd.
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

package org.secretflow.dataproxy.plugin.odps.reader;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.utils.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.secretflow.dataproxy.core.converter.BigIntVectorConverter;
import org.secretflow.dataproxy.core.converter.BitVectorConverter;
import org.secretflow.dataproxy.core.converter.DateDayVectorConverter;
import org.secretflow.dataproxy.core.converter.DateMilliVectorConverter;
import org.secretflow.dataproxy.core.converter.Float4VectorConverter;
import org.secretflow.dataproxy.core.converter.Float8VectorConverter;
import org.secretflow.dataproxy.core.converter.IntVectorConverter;
import org.secretflow.dataproxy.core.converter.SmallIntVectorConverter;
import org.secretflow.dataproxy.core.converter.TimeMilliVectorConvertor;
import org.secretflow.dataproxy.core.converter.TimeStampNanoVectorConverter;
import org.secretflow.dataproxy.core.converter.TinyIntVectorConverter;
import org.secretflow.dataproxy.core.converter.ValueConversionStrategy;
import org.secretflow.dataproxy.core.converter.VarCharVectorConverter;
import org.secretflow.dataproxy.core.reader.AbstractSender;
import org.secretflow.dataproxy.core.visitor.BooleanValueVisitor;
import org.secretflow.dataproxy.core.visitor.ByteArrayValueVisitor;
import org.secretflow.dataproxy.core.visitor.ByteValueVisitor;
import org.secretflow.dataproxy.core.visitor.DoubleValueVisitor;
import org.secretflow.dataproxy.core.visitor.FloatValueVisitor;
import org.secretflow.dataproxy.core.visitor.IntegerValueVisitor;
import org.secretflow.dataproxy.core.visitor.LongValueVisitor;
import org.secretflow.dataproxy.core.visitor.ShortValueVisitor;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author yuexie
 * @date 2024/11/1 14:14
 **/
@Slf4j
public class OdpsRecordSender extends AbstractSender<Record> {

    private final static Map<ArrowType.ArrowTypeID, ValueConversionStrategy> ARROW_TYPE_ID_FIELD_CONSUMER_MAP = new HashMap<>();

    private final Map<String, FieldVector> fieldVectorMap = new HashMap<>();

    private boolean isInit = false;

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
     * @param estimatedRecordCount estimated record count
     * @param recordQueue          record queue
     * @param root                 vector schema root
     */
    public OdpsRecordSender(int estimatedRecordCount, LinkedBlockingQueue<Record> recordQueue, VectorSchemaRoot root) {
        super(estimatedRecordCount, recordQueue, root);
    }

    @Override
    protected void toArrowVector(Record record, @Nonnull VectorSchemaRoot root, int takeRecordCount) {

        log.trace("record: {}, takeRecordCount: {}", record, takeRecordCount);

        this.initRecordColumn2FieldMap(record);

        Optional<FieldVector> filedVectorOpt;
        FieldVector vector;
        String columnName;
        ArrowType.ArrowTypeID arrowTypeID;
        Column[] recordColumns = record.getColumns();
        Object recordColumnValue;

        for (Column recordColumn : recordColumns) {
            columnName = recordColumn.getName();

            filedVectorOpt = Optional.ofNullable(this.fieldVectorMap.get(columnName));

            if (filedVectorOpt.isPresent()) {
                vector = filedVectorOpt.get();
                recordColumnValue = record.get(columnName);

                if (Objects.isNull(recordColumnValue)) {
                    vector.setNull(takeRecordCount);
                    continue;
                }

                arrowTypeID = vector.getField().getType().getTypeID();
                ArrowType.ArrowTypeID finalArrowTypeID = arrowTypeID;

                ValueConversionStrategy fieldConsumer =
                        Optional.ofNullable(ARROW_TYPE_ID_FIELD_CONSUMER_MAP.get(arrowTypeID))
                                .orElseThrow(() -> new RuntimeException("Unsupported arrow type id: " + finalArrowTypeID));

                fieldConsumer.convertAndSet(vector, takeRecordCount, recordColumnValue);

            } else {
                log.debug("columnName: {} not in needColumns", columnName);
            }
        }

    }

    @Override
    protected boolean isOver(Record record) {
        return record instanceof ArrayRecord && record.getColumns().length == 0;
    }

    @Override
    public void putOver() throws InterruptedException {
        this.put(new ArrayRecord(new Column[0]));
        log.debug("putOver");
    }

    private synchronized void initRecordColumn2FieldMap(Record record) {

        if (isInit) {
            return;
        }

        VectorSchemaRoot root = getRoot();

        if (Objects.isNull(root)) {
            return;
        }
        List<FieldVector> fieldVectors = root.getFieldVectors();
        Column[] columns = record.getColumns();
        Optional<FieldVector> first;
        for (Column column : columns) {
            String name = column.getName();
            first = fieldVectors.stream()
                    .filter(fieldVector -> StringUtils.equalsIgnoreCase(fieldVector.getName(), name))
                    .findFirst();

            if (first.isPresent()) {
                fieldVectorMap.put(name, first.get());
            } else {
                log.debug("columnName: {} not in fieldVectors", name);
            }
        }
        isInit = true;
    }
}
