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

import org.secretflow.dataproxy.plugin.database.config.TaskConfig;
import org.secretflow.dataproxy.plugin.database.utils.Record;
import org.secretflow.dataproxy.core.reader.AbstractReader;
import org.secretflow.dataproxy.core.reader.Sender;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DatabaseRecordReader extends AbstractReader<TaskConfig, Record> {

    private final ResultSet resultSet;
    public DatabaseRecordReader(TaskConfig param, Sender<Record> sender, ResultSet rs){
        super(param, sender);
        this.resultSet = rs;
    }

    @Override
    protected void read(TaskConfig param) {
        int recordCount = 0;
        Record record;
        try {
            while(resultSet.next()) {
                record = new Record(resultSet);
                recordCount++;
                this.put(record);
            }

            // 最后一个放入空的record设置last的tag，
            record = new Record();
            record.setLast(true);
            this.put(record);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
