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

package org.secretflow.dataproxy.integration.tests;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author muhong
 * @date 2023-10-08 15:33
 */
public class TestDataUtils {

    private final static double nullRate = 0.2;
    private final static Random random = new Random(System.currentTimeMillis());

    /**
     * 生成kuscia全类型测试数据
     *
     * @param allocator
     * @return
     */
    public static VectorSchemaRoot generateKusciaAllTypeData(BufferAllocator allocator) {
        List<FieldVector> vectorList = new ArrayList<>();
        int count = 10;

        vectorList.add(createTinyIntVector(allocator, count));
        vectorList.add(createSmallIntVector(allocator, count));
        vectorList.add(createIntVector(allocator, count));
        vectorList.add(createBigIntVector(allocator, count));
        vectorList.add(createFloat4Vector(allocator, count));
        vectorList.add(createFloat8Vector(allocator, count));
        vectorList.add(createBitVector(allocator, count));
        vectorList.add(createVarCharVector(allocator, count));
        vectorList.add(createVarBinaryVector(allocator, count));

        VectorSchemaRoot result = new VectorSchemaRoot(vectorList);
        result.setRowCount(count);
        result.syncSchema();
        return result;
    }

    public static TinyIntVector createTinyIntVector(BufferAllocator allocator, int count) {
        TinyIntVector tinyIntVector = new TinyIntVector("tinyint", allocator);
        tinyIntVector.allocateNew(count);
        for (int i = 0; i < count; i++) {
            if (random.nextInt() % 100 < (nullRate * 100)) {
                tinyIntVector.setNull(i);
            } else {
                tinyIntVector.set(i, i % 128);
            }
        }
        return tinyIntVector;
    }

    public static SmallIntVector createSmallIntVector(BufferAllocator allocator, int count) {
        SmallIntVector smallIntVector = new SmallIntVector("smallint", allocator);
        smallIntVector.allocateNew(count);
        for (int i = 0; i < count; i++) {
            if (random.nextInt() % 100 < (nullRate * 100)) {
                smallIntVector.setNull(i);
            } else {
                smallIntVector.set(i, i % 32768);
            }
        }
        return smallIntVector;
    }

    public static IntVector createIntVector(BufferAllocator allocator, int count) {
        IntVector intVector = new IntVector("int", allocator);
        intVector.allocateNew(count);
        for (int i = 0; i < count; i++) {
            if (random.nextInt() % 100 < (nullRate * 100)) {
                intVector.setNull(i);
            } else {
                intVector.set(i, i);
            }
        }
        return intVector;
    }

    public static BigIntVector createBigIntVector(BufferAllocator allocator, int count) {
        BigIntVector bigIntVector = new BigIntVector("bigInt", allocator);
        bigIntVector.allocateNew(count);
        for (int i = 0; i < count; i++) {
            if (random.nextInt() % 100 < (nullRate * 100)) {
                bigIntVector.setNull(i);
            } else {
                bigIntVector.set(i, i);
            }
        }
        return bigIntVector;
    }

    public static UInt1Vector createUInt1Vector(BufferAllocator allocator, int count) {
        UInt1Vector uInt1Vector = new UInt1Vector("uint1", allocator);
        uInt1Vector.allocateNew(count);
        for (int i = 0; i < count; i++) {
            if (random.nextInt() % 100 < (nullRate * 100)) {
                uInt1Vector.setNull(i);
            } else {
                uInt1Vector.set(i, i % 256);
            }
        }
        return uInt1Vector;
    }

    public static UInt2Vector createUInt2Vector(BufferAllocator allocator, int count) {
        UInt2Vector uInt2Vector = new UInt2Vector("uint2", allocator);
        uInt2Vector.allocateNew(count);
        for (int i = 0; i < count; i++) {
            if (random.nextInt() % 100 < (nullRate * 100)) {
                uInt2Vector.setNull(i);
            } else {
                uInt2Vector.set(i, i % 65536);
            }
        }
        return uInt2Vector;
    }

    public static UInt4Vector createUInt4Vector(BufferAllocator allocator, int count) {
        UInt4Vector uInt4Vector = new UInt4Vector("uint4", allocator);
        uInt4Vector.allocateNew(count);
        for (int i = 0; i < count; i++) {
            if (random.nextInt() % 100 < (nullRate * 100)) {
                uInt4Vector.setNull(i);
            } else {
                uInt4Vector.set(i, i);
            }
        }
        return uInt4Vector;
    }

    public static UInt8Vector createUInt8Vector(BufferAllocator allocator, int count) {
        UInt8Vector uInt8Vector = new UInt8Vector("unit8", allocator);
        uInt8Vector.allocateNew(count);
        for (int i = 0; i < count; i++) {
            if (random.nextInt() % 100 < (nullRate * 100)) {
                uInt8Vector.setNull(i);
            } else {
                uInt8Vector.set(i, i);
            }
        }
        return uInt8Vector;
    }

    public static Float4Vector createFloat4Vector(BufferAllocator allocator, int count) {
        Float4Vector float4Vector = new Float4Vector("float4", allocator);
        float4Vector.allocateNew(count);
        for (int i = 0; i < count; i++) {
            if (random.nextInt() % 100 < (nullRate * 100)) {
                float4Vector.setNull(i);
            } else {
                float4Vector.set(i, random.nextFloat());
            }
        }
        return float4Vector;
    }

    public static Float8Vector createFloat8Vector(BufferAllocator allocator, int count) {
        Float8Vector float8Vector = new Float8Vector("double", allocator);
        float8Vector.allocateNew(count);
        for (int i = 0; i < count; i++) {
            if (random.nextInt() % 100 < (nullRate * 100)) {
                float8Vector.setNull(i);
            } else {
                float8Vector.set(i, random.nextDouble());
            }
        }
        return float8Vector;
    }

    public static BitVector createBitVector(BufferAllocator allocator, int count) {
        BitVector bitVector = new BitVector("bool", allocator);
        bitVector.allocateNew(count);
        for (int i = 0; i < count; i++) {
            bitVector.set(i, random.nextBoolean() ? 1 : 0);
        }
        return bitVector;
    }

    public static VarCharVector createVarCharVector(BufferAllocator allocator, int count) {
        VarCharVector varCharVector = new VarCharVector("string", allocator);
        varCharVector.allocateNew(count);
        for (int i = 0; i < count; i++) {
            if (random.nextInt() % 100 < (nullRate * 100)) {
                varCharVector.setNull(i);
            } else {
                varCharVector.set(i, ("string_" + i).getBytes(StandardCharsets.UTF_8));
            }
        }
        return varCharVector;
    }

    public static VarBinaryVector createVarBinaryVector(BufferAllocator allocator, int count) {
        VarBinaryVector binaryVector = new VarBinaryVector("binary", allocator);
        binaryVector.allocateNew(count);
        for (int i = 0; i < count; i++) {
            if (random.nextInt() % 100 < (nullRate * 100)) {
                binaryVector.setNull(i);
            } else {
                binaryVector.set(i, ("binary_" + i).getBytes(StandardCharsets.UTF_8));
            }
        }
        return binaryVector;
    }
}