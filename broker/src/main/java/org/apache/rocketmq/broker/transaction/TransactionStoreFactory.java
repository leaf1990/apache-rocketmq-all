package org.apache.rocketmq.broker.transaction;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.broker.transaction.tddl.TddlTransactionStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Created by diwayou on 17-3-16.
 */
public class TransactionStoreFactory {

    public enum StoreType {
        TDDL, NONE
    }

    public static TransactionStore getTransactionStore(MessageStoreConfig config) {

        StoreType storeType = StoreType.valueOf(config.getTransactionStoreType());

        switch (storeType) {
            case TDDL:
                if (StringUtils.isEmpty(config.getAppName()) || StringUtils.isEmpty(config.getDbGroupKey())) {
                    throw new RuntimeException("Must config appName and dbGroupKey when use TDDL for transaction log.");
                }

                return new TddlTransactionStore(config);
            case NONE:
                return new NoneTransactionStore();
            default:
                throw new RuntimeException("No transaction implementation for " + storeType);
        }
    }

    public static class NoneTransactionStore implements TransactionStore {

        @Override
        public boolean open() {
            return true;
        }

        @Override
        public boolean computeTotalRecords() {
            return false;
        }

        @Override
        public void close() {

        }

        @Override
        public boolean put(Set<TransactionRecord> trs) {
            return true;
        }

        @Override
        public void remove(Set<Long> pks) {

        }

        @Override
        public List<TransactionRecord> traverse(long pk, int nums) {
            return Collections.emptyList();
        }

        @Override
        public long totalRecords() {
            return 0;
        }

        @Override
        public long minPK() {
            return 0;
        }

        @Override
        public long maxPK() {
            return 0;
        }
    }
}
