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
package org.secretflow.dataproxy.plugin.odps.io;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author yuexie
 * @date 2025/2/12 15:58
 **/
@ExtendWith(MockitoExtension.class)
public class DynamicSequenceInputStreamTest {

    @InjectMocks
    private DynamicSequenceInputStream dynamicSequenceInputStream;

    @Test
    void testRead() throws Exception {

        byte[] data1 = "Hello".getBytes();
        byte[] data2 = "World".getBytes();
        byte[] data3 = "!".getBytes();

        dynamicSequenceInputStream.appendStream(new ByteArrayInputStream(data1));
        dynamicSequenceInputStream.appendStream(new ByteArrayInputStream(data2));
        dynamicSequenceInputStream.appendStream(new ByteArrayInputStream(data3));
        dynamicSequenceInputStream.setCompleted();

        StringBuilder result = new StringBuilder();
        int b;
        while ((b = dynamicSequenceInputStream.read()) != -1) {
            result.append((char) b);
        }
        assertEquals("HelloWorld!", result.toString());
    }

    @Test
    void testEmptyList() throws Exception {
        dynamicSequenceInputStream.setCompleted();
        assertEquals(-1, dynamicSequenceInputStream.read());
    }

    @Test
    void testSingleStream() throws Exception {
        byte[] data1 = "Hello".getBytes();
        dynamicSequenceInputStream.appendStream(new ByteArrayInputStream(data1));
        dynamicSequenceInputStream.setCompleted();

        StringBuilder result = new StringBuilder();
        int b;
        while ((b = dynamicSequenceInputStream.read()) != -1) {
            result.append((char) b);
        }
        assertEquals("Hello", result.toString());
    }

    @Test
    void testClose() throws Exception {

        InputStream mockInputStream1 = mock(InputStream.class);
        InputStream mockInputStream2 = mock(InputStream.class);
        InputStream mockInputStream3 = mock(InputStream.class);

        dynamicSequenceInputStream.appendStream(mockInputStream1);
        dynamicSequenceInputStream.appendStream(mockInputStream2);
        dynamicSequenceInputStream.appendStream(mockInputStream3);
        dynamicSequenceInputStream.setCompleted();

        dynamicSequenceInputStream.close();

        verify(mockInputStream1, atLeastOnce()).close();
        verify(mockInputStream2, atLeastOnce()).close();
        verify(mockInputStream3, atLeastOnce()).close();
    }
}
