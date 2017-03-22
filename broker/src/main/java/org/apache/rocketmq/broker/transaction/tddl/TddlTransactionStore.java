package org.apache.rocketmq.broker.transaction.tddl;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.group.jdbc.TGroupDataSource;
import org.apache.rocketmq.broker.transaction.TransactionRecord;
import org.apache.rocketmq.broker.transaction.TransactionStore;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Created by diwayou on 17-3-16.
 */
public class TddlTransactionStore implements TransactionStore {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private static final String INSERT = "insert into transaction_log values (?, ?, ?)";

    private static final String DELETE = "DELETE FROM transaction_log WHERE offset = ?";

    private static final String TRAVERSE = "select offset, producer_group, gmt_create from transaction_log where offset > ? order by offset asc limit ?";

    private MessageStoreConfig config;

    private TGroupDataSource dataSource;

    public TddlTransactionStore(MessageStoreConfig config) {
        this.config = config;
        this.dataSource = new TGroupDataSource(config.getDbGroupKey(), config.getAppName());
    }

    @Override
    public boolean open() {
        try {
            dataSource.init();
            log.info("TDDL transaction store open success.");
        } catch (TddlException e) {
            log.error("open transaction store fail: ", e);
            return false;
        }

        return true;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean put(Set<TransactionRecord> trs) {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            statement = connection.prepareStatement(INSERT);
            for (TransactionRecord tr : trs) {
                statement.setLong(1, tr.getOffset());
                statement.setString(2, tr.getProducerGroup());
                statement.setTimestamp(3, tr.getGmtCreate());
                statement.addBatch();
            }
            int[] executeBatch = statement.executeBatch();
            connection.commit();
            return true;
        } catch (Exception e) {
            log.warn("createDB Exception", e);
            return false;
        } finally {
            closeStatement(statement);
            closeConnection(connection);
        }
    }

    @Override
    public void remove(Set<Long> pks) {
        if (pks.isEmpty()) return;

        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            statement = connection.prepareStatement(DELETE);
            for (long pk : pks) {
                statement.setLong(1, pk);
                statement.addBatch();
            }
            int[] executeBatch = statement.executeBatch();
            connection.commit();
        } catch (Exception e) {
            log.warn("createDB Exception", e);
        } finally {
            closeStatement(statement);
            closeConnection(connection);
        }
    }

    @Override
    public List<TransactionRecord> traverse(long pk, int nums) {
        PreparedStatement statement = null;
        Connection connection = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement(TRAVERSE);

            statement.setLong(1, pk);
            statement.setInt(2, nums);

            resultSet = statement.executeQuery();

            List<TransactionRecord> result = new ArrayList<>(nums);
            while (resultSet.next()) {
                TransactionRecord tr = new TransactionRecord();
                tr.setOffset(resultSet.getLong(1));
                tr.setProducerGroup(resultSet.getString(2));
                tr.setGmtCreate(resultSet.getTimestamp(3));

                result.add(tr);
            }

            return result;
        } catch (Exception e) {
            log.warn("traverse fail: ", e);

            return Collections.emptyList();
        } finally {
            closeResultSet(resultSet);
            closeStatement(statement);
            closeConnection(connection);
        }
    }

    private static void closeResultSet(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not close JDBC ResultSet", e);
            } catch (Throwable e) {
                log.debug("Unexpected exception on closing JDBC ResultSet", e);
            }
        }
    }

    private static void closeStatement(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                log.debug("Could not close JDBC Statement", e);
            } catch (Throwable e) {
                log.debug("Unexpected exception on closing JDBC Statement", e);
            }
        }
    }

    private static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                log.debug("Could not close JDBC Connection", e);
            } catch (Throwable e) {
                log.debug("Unexpected exception on closing JDBC Connection", e);
            }
        }
    }
}
