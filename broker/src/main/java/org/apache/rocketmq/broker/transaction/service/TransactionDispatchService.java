package org.apache.rocketmq.broker.transaction.service;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.transaction.TransactionRecord;
import org.apache.rocketmq.broker.transaction.TransactionStore;
import org.apache.rocketmq.broker.transaction.TransactionStoreFactory;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

/**
 * Created by diwayou on 17-3-16.
 */
public class TransactionDispatchService extends ServiceThread {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private DefaultMessageStore defaultMessageStore;

    private TransactionStore transactionStore;

    private TransactionStateService transactionStateService;

    private ExecutorService executorService;

    private volatile ConcurrentMap<String, List<DispatchRequest>> producerToRequestMapRead = new ConcurrentHashMap<>();

    private volatile ConcurrentMap<String, List<DispatchRequest>> producerToRequestMapWrite = new ConcurrentHashMap<>();

    public TransactionDispatchService(BrokerController brokerController, DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.executorService = Executors.newFixedThreadPool(defaultMessageStore.getMessageStoreConfig().getTransactionAsyncPoolSize(),
                new ThreadFactoryImpl("TransactionDispatchService-"));
        this.transactionStore = TransactionStoreFactory.getTransactionStore(defaultMessageStore.getMessageStoreConfig());
        this.transactionStateService = new TransactionStateService(brokerController, defaultMessageStore, transactionStore);
    }

    public void putRequest(DispatchRequest request) {
        String producerGroup = request.getProducerGroup();
        int transactionLogAccumulateSize = defaultMessageStore.getMessageStoreConfig().getTransactionLogAccumulateSize();

        do {
            List<DispatchRequest> requests = producerToRequestMapWrite.computeIfAbsent(producerGroup, k -> Collections.synchronizedList(new LinkedList<>()));

            if (requests.size() > transactionLogAccumulateSize) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("TransactionDispatchService exception: ", e);
                }
            } else {
                requests.add(request);

                // 如果有消息，需要唤起处理线程
                this.wakeup();

                break;
            }
        } while (true);
    }

    @Override
    public String getServiceName() {
        return TransactionDispatchService.class.getSimpleName();
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                this.waitForRunning(0);
                this.doDispatch();
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        // 在正常shutdown情况下，要保证所有消息都dispatch
        try {
            Thread.sleep(5 * 1000);
        } catch (InterruptedException e) {
            log.warn("TransactionDispatchService Exception, ", e);
        }

        synchronized (this) {
            this.swapRequests();
        }

        this.doDispatch();

        log.info(this.getServiceName() + " service end");
    }

    @Override
    protected void onWaitEnd() {
        this.swapRequests();
    }

    private void swapRequests() {
        ConcurrentMap<String, List<DispatchRequest>> tmp = this.producerToRequestMapWrite;
        this.producerToRequestMapWrite = this.producerToRequestMapRead;
        this.producerToRequestMapRead = tmp;
    }

    private void doDispatch() {
        if (producerToRequestMapRead.isEmpty()) return;

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        long transactionTimestamp = 0;
        for (String producerGroup : producerToRequestMapRead.keySet()) {
            List<DispatchRequest> dispatchRequests = producerToRequestMapRead.remove(producerGroup);
            if (dispatchRequests == null || dispatchRequests.isEmpty()) continue;

            Set<TransactionRecord> prepare = new HashSet<>();
            Set<Long> rollbackOrCommit = new HashSet<>();
            for (DispatchRequest request : dispatchRequests) {
                // 记录该批数据最大的存储时间，当事务消息持久化完，需要记录对应的checkpoint
                if (request.getStoreTimestamp() > transactionTimestamp) {
                    transactionTimestamp = request.getStoreTimestamp();
                }

                final int tranType = MessageSysFlag.getTransactionValue(request.getSysFlag());
                switch (tranType) {
                    case MessageSysFlag.TRANSACTION_NOT_TYPE:
                        break;
                    case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                        prepare.add(new TransactionRecord(request.getCommitLogOffset(),
                                request.getProducerGroup(),
                                new Timestamp(request.getStoreTimestamp())));
                        break;
                    case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                        TransactionRecord record = new TransactionRecord(request.getPreparedTransactionOffset(),
                                request.getProducerGroup(),
                                new Timestamp(request.getStoreTimestamp()));
                        if (prepare.contains(record)) {
                            prepare.remove(record);
                        } else {
                            rollbackOrCommit.add(record.getOffset());
                        }
                        break;
                }
            }

            futures.add(CompletableFuture.runAsync(() ->
                            runUntilSuccess(() -> persistentPrepare(prepare)),
                    executorService));
            futures.add(CompletableFuture.runAsync(() ->
                            runUntilSuccess(() -> persistentRollbackOrCommit(rollbackOrCommit)),
                    executorService));
        }

        for (CompletableFuture<Void> completableFuture : futures) {
            completableFuture.join();
        }

        if (transactionTimestamp > 0) {
            defaultMessageStore.getStoreCheckpoint().setTransactionTimestamp(transactionTimestamp);
        }
    }

    private boolean persistentPrepare(Set<TransactionRecord> prepare) {
        return transactionStore.put(prepare);
    }

    private boolean persistentRollbackOrCommit(Set<Long> rollbackOrCommit) {
        transactionStore.remove(rollbackOrCommit);

        return true;
    }

    private void runUntilSuccess(Supplier<Boolean> supplier) {
        boolean success = false;
        do {
            try {
                if (supplier.get()) {
                    success = true;
                }
            } catch (Exception e) {
                log.error("run fail: ", e);

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    log.warn("exception: ", e1);
                }
            }
        } while (!success);
    }

    @Override
    public void start() {
        if (!this.transactionStore.open()) {
            throw new RuntimeException("open store fail.");
        }

        this.transactionStateService.start();
        log.info("transaction state service start success.");

        super.start();
    }

    @Override
    public void shutdown() {
        executorService.shutdown();
        try {
            executorService.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.warn("shutdown exception: ", e);
        }

        executorService.shutdownNow();

        this.transactionStateService.shutdown();
        this.transactionStore.close();

        super.shutdown();
    }
}
