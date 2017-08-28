package org.apache.rocketmq.broker.transaction.service;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.transaction.TransactionRecord;
import org.apache.rocketmq.broker.transaction.TransactionStore;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.BrokerRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Created by diwayou on 17-3-17.
 */
public class TransactionStateService {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private final BrokerController brokerController;

    private final DefaultMessageStore messageStore;

    private final TransactionStore transactionStore;

    private ScheduledExecutorService scheduledExecutorService;

    public TransactionStateService(BrokerController brokerController, DefaultMessageStore messageStore, TransactionStore transactionStore) {
        this.brokerController = brokerController;
        this.messageStore = messageStore;
        this.transactionStore = transactionStore;
    }

    public void start() {
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("TransactionCheckSchedule"));

        initScheduleTask();
    }

    private void initScheduleTask() {
        scheduledExecutorService.scheduleWithFixedDelay(this::doSchedule,
                60,
                messageStore.getMessageStoreConfig().getCheckScheduleIntervalSeconds(),
                TimeUnit.SECONDS);
    }

    private static boolean recordTooOld(TransactionRecord transactionRecord, int checkSecondsBefore) {
        return transactionRecord.getGmtCreate().before(DateUtils.addSeconds(new Date(), -checkSecondsBefore));
    }

    private void doSchedule() {
        boolean slave = messageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE;
        int checkSecondsBefore = messageStore.getMessageStoreConfig().getCheckTransactionLogSecondsBefore();

        if (slave) {
            return;
        }

        int pageSize = messageStore.getMessageStoreConfig().getCheckPageSize();
        Map<String, List<ClientChannelInfo>> clientsCache = Maps.newHashMap();
        long offsetBegin = 0;
        boolean finished = false;
        do {
            List<TransactionRecord> transactionRecords = transactionStore.traverse(offsetBegin, pageSize);
            if (transactionRecords == null || transactionRecords.isEmpty()) {
                break;
            }

            for (TransactionRecord transactionRecord : transactionRecords) {
                if (!recordTooOld(transactionRecord, checkSecondsBefore)) {
                    finished = true;
                    break;
                }

                try {
                    checkTransactionRecord(transactionRecord,
                            clientsCache.computeIfAbsent(transactionRecord.getProducerGroup(),
                                    k -> brokerController.getProducerManager().getClientChannelInfo(k)));
                } catch (Exception e) {
                    log.warn("check fail: ", e);
                }
            }

            if (transactionRecords.size() < pageSize) {
                break;
            }

            offsetBegin = transactionRecords.get(transactionRecords.size() - 1).getOffset();
        } while (!finished);
    }

    private void checkTransactionRecord(TransactionRecord transactionRecord, List<ClientChannelInfo> clientChannelInfoList) {
        ClientChannelInfo clientChannelInfo = randomChoose(clientChannelInfoList);
        if (clientChannelInfo == null) {
            log.info("no client for check transaction. group={}", transactionRecord.getProducerGroup());
            return;
        }

        SelectMappedBufferResult selectMappedBufferResult =
                this.brokerController.getMessageStore().selectOneMessageByOffset(transactionRecord.getOffset());
        if (null == selectMappedBufferResult) {
            log.warn("check a producer transaction state, but not find message by commitLogOffset: {}, group: ",
                    transactionRecord.getOffset(), transactionRecord.getProducerGroup());

            // TODO remove transaction log?
            this.transactionStore.remove(Collections.singleton(transactionRecord.getOffset()));
            return;
        }

        final CheckTransactionStateRequestHeader requestHeader = new CheckTransactionStateRequestHeader();
        requestHeader.setCommitLogOffset(transactionRecord.getOffset());
        requestHeader.setTranStateTableOffset(transactionRecord.getOffset());

        this.brokerController.getBroker2Client().checkProducerTransactionState(
                clientChannelInfo.getChannel(), requestHeader, selectMappedBufferResult);
    }

    private ClientChannelInfo randomChoose(List<ClientChannelInfo> clientChannelInfoList) {
        if (clientChannelInfoList == null || clientChannelInfoList.isEmpty()) return null;

        int index = this.generateRandomNum() % clientChannelInfoList.size();
        ClientChannelInfo info = clientChannelInfoList.get(index);

        return info;
    }

    private int generateRandomNum() {
        int value = ThreadLocalRandom.current().nextInt();

        if (value < 0) {
            value = Math.abs(value);
        }

        return value;
    }

    public void shutdown() {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
    }
}
