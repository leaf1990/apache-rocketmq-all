package org.apache.rocketmq.store.transaction;

import org.apache.rocketmq.store.DispatchRequest;

/**
 * Created by diwayou on 17-3-20.
 */
public interface TransactionLogPersistListener {

    void persist(DispatchRequest request);
}
