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

package org.secretflow.dataproxy.core.reader;

import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.VariableWidthVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.ValueVectorUtility;

import java.io.Closeable;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author yuexie
 * @date 2024/11/1 11:00
 **/
@Slf4j
public abstract class AbstractSender<T> implements Sender<T>, AutoCloseable {

    /**
     * Queue, used to store records to be sent
     */
    private final LinkedBlockingQueue<T> recordQueue;

    private final VectorSchemaRoot root;

    /**
     * Estimated number of records to be sent
     */
    private final int estimatedRecordCount;

    /**
     * Constructor
     *
     * @param estimatedRecordCount Estimated number of records to be sent
     * @param recordQueue          Queue, used to store records to be sent
     * @param root                 Arrow vector schema root
     */
    public AbstractSender(int estimatedRecordCount, LinkedBlockingQueue<T> recordQueue, VectorSchemaRoot root) {
        this.recordQueue = recordQueue;
        this.root = root;
        this.estimatedRecordCount = estimatedRecordCount;
        this.preAllocate();
    }

    @Override
    public void put(T record) throws InterruptedException {
        recordQueue.put(record);
        log.trace("recordQueue size: {}, recordQueue offer record: {}", recordQueue.size(), record);
    }

    @Override
    public void send() {
        preAllocate();
        log.info("start send");
        try {
            int takeRecordCount = 0;

            for (;;) {
                T record = recordQueue.take();
                log.trace("recordQueue take record: {}", record);
                if (isOver(record)) {
                    log.debug("recordQueue take record take Count: {}", takeRecordCount);
                    break;
                }
                ValueVectorUtility.ensureCapacity(root, takeRecordCount + 1);
                this.toArrowVector(record, root, takeRecordCount);
                takeRecordCount++;

                if (takeRecordCount % 300_000 == 0) {
                    break;
                }
            }
            root.setRowCount(takeRecordCount);
            log.info("send record take Count: {}", takeRecordCount);
        } catch (InterruptedException e) {
            log.error("send record interrupted", e);
            Thread.currentThread().interrupt();
        }
    }

    protected abstract void toArrowVector(T record, VectorSchemaRoot root, int takeRecordCount);

    protected abstract boolean isOver(T record);

    protected VectorSchemaRoot getRoot() {
        return root;
    }


    /**
     * Closes this resource, relinquishing any underlying resources.
     * This method is invoked automatically on objects managed by the
     * {@code try}-with-resources statement.
     *
     * @throws Exception if this resource cannot be closed
     * @apiNote While this interface method is declared to throw {@code
     * Exception}, implementers are <em>strongly</em> encouraged to
     * declare concrete implementations of the {@code close} method to
     * throw more specific exceptions, or to throw no exception at all
     * if the close operation cannot fail.
     *
     * <p> Cases where the close operation may fail require careful
     * attention by implementers. It is strongly advised to relinquish
     * the underlying resources and to internally <em>mark</em> the
     * resource as closed, prior to throwing the exception. The {@code
     * close} method is unlikely to be invoked more than once and so
     * this ensures that the resources are released in a timely manner.
     * Furthermore it reduces problems that could arise when the resource
     * wraps, or is wrapped, by another resource.
     *
     * <p><em>Implementers of this interface are also strongly advised
     * to not have the {@code close} method throw {@link
     * InterruptedException}.</em>
     * <p>
     * This exception interacts with a thread's interrupted status,
     * and runtime misbehavior is likely to occur if an {@code
     * InterruptedException} is {@linkplain Throwable#addSuppressed
     * suppressed}.
     * <p>
     * More generally, if it would cause problems for an
     * exception to be suppressed, the {@code AutoCloseable.close}
     * method should not throw it.
     *
     * <p>Note that unlike the {@link Closeable#close close}
     * method of {@link Closeable}, this {@code close} method
     * is <em>not</em> required to be idempotent.  In other words,
     * calling this {@code close} method more than once may have some
     * visible side effect, unlike {@code Closeable.close} which is
     * required to have no effect if called more than once.
     * <p>
     * However, implementers of this interface are strongly encouraged
     * to make their {@code close} methods idempotent.
     */
    @Override
    public void close() throws Exception {
        if (recordQueue != null) {
            recordQueue.clear();
        }
    }

    /**
     * Pre-application for arrow vector memory
     */
    private void preAllocate() {

        ValueVectorUtility.preAllocate(root, estimatedRecordCount);

        root.getFieldVectors().forEach(fieldVector -> {
            if (fieldVector instanceof FixedWidthVector baseFixedWidthVector) {
                baseFixedWidthVector.allocateNew(estimatedRecordCount);
            } else if (fieldVector instanceof VariableWidthVector baseVariableWidthVector) {
                baseVariableWidthVector.allocateNew(estimatedRecordCount * 32);
            }
        });
        root.clear();
    }
}
