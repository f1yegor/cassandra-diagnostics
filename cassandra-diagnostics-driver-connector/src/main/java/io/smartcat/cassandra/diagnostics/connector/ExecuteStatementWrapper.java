package io.smartcat.cassandra.diagnostics.connector;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;

import io.smartcat.cassandra.diagnostics.Query;

/**
 * This class is a Diagnostics wrapper for
 * {@link com.datastax.driver.core.SessionManager#executeAsync(com.datastax.driver.core.Statement)}.
 */
public class ExecuteStatementWrapper {

    private static final Logger logger = LoggerFactory.getLogger(ExecuteStatementWrapper.class);

    /**
     * The number of threads used for executing query reports.
     */
    private static final int EXECUTOR_NO_THREADS = 2;

    private static final AtomicLong THREAD_COUNT = new AtomicLong(0);

    /**
     * Executor service used for executing query reports.
     */
    private static ExecutorService executor = Executors.newFixedThreadPool(EXECUTOR_NO_THREADS,
            new ThreadFactory() {
                @Override
                public Thread newThread(Runnable runnable) {
                    Thread thread = new Thread(runnable);
                    thread.setName("cassandra-diagnostics-connector-" + THREAD_COUNT.getAndIncrement());
                    thread.setDaemon(true);
                    thread.setPriority(Thread.MIN_PRIORITY);
                    return thread;
                }
            });

    private QueryReporter queryReporter;
    private final String host;

    /**
     * Constructor.
     *
     * @param queryReporter QueryReporter used to report queries
     */
    public ExecuteStatementWrapper(QueryReporter queryReporter) {
        this.queryReporter = queryReporter;

        // obtain host address
        String hostAddress;
        try {
            hostAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            hostAddress = "UNKNOWN";
        }
        host = hostAddress;
    }

    /**
     * Wrapper for
     * {@link com.datastax.driver.core.SessionManager#executeAsync(Statement)} method.
     * This method wraps the original method and, in addition, measures the statement
     * execution time and reports the query towards the diagnostics core.
     *
     * @param statement Statement
     * @param startTime execution start time
     * @param result execution's result future
     */
    public void processStatement(final Statement statement, long startTime, ResultSetFuture result) {
        report(startTime, statement, result);
    }

    /**
     * Submits a query reports asynchronously.
     *
     * @param startTime    execution start time, in milliseconds
     * @param statement    CQL statement
     * @param result   ResultSetFuture
     */
    private void report(final long startTime, final Statement statement, final ResultSetFuture result) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    // wait for the statement to be executed
                    result.getUninterruptibly();
                    final long execTime = System.currentTimeMillis() - startTime;
                    Query query = extractQuery(startTime, execTime, statement);
                    logger.trace("Reporting query: {}.", query);
                    queryReporter.report(query);
                } catch (Exception e) {
                    logger.warn("An error occured while reporting query", e);
                }
            }
        });
    }

    private Query extractQuery(final long startTime, final long execTime, final Statement statement) {
        final String queryString = statementQueryString(statement);
        final Query.StatementType queryType = queryType(queryString);
        return Query.create(
                startTime,
                execTime,
                host,
                queryType,
                statement.getKeyspace(),
                "",
                queryString,
                "");
    }

    private String statementQueryString(final Statement statement) {
        String query;
        if (statement instanceof RegularStatement) {
            query = statementQueryString((RegularStatement) statement);
        } else if (statement instanceof BoundStatement) {
            query = statementQueryString((BoundStatement) statement);
        } else if (statement instanceof BatchStatement) {
            query = statementQueryString((BatchStatement) statement);
        } else {
            query = "unknown;";
        }
        return query;
    }

    private String statementQueryString(final RegularStatement statement) {
        return statement.getQueryString() + ";";
    }

    private String statementQueryString(final BoundStatement statement) {
        return statement.preparedStatement().getQueryString() + ";";
    }

    private String statementQueryString(final BatchStatement batchStatement) {
        StringBuffer sb = new StringBuffer();
        sb.append("BEGIN BATCH ");
        for (Statement statement : batchStatement.getStatements()) {
            sb.append(statementQueryString(statement));
        }
        sb.append(" APPLY BATCH;");
        return sb.toString();
    }

    private Query.StatementType queryType(final String query) {
        final Query.StatementType type;
        final String normalizedQuery = query.toUpperCase();
        if (normalizedQuery.toUpperCase().startsWith("SELECT")) {
            type = Query.StatementType.SELECT;
        } else if (normalizedQuery.startsWith("INSERT") || normalizedQuery.startsWith("UPDATE")
                || normalizedQuery.startsWith("BEGIN")) {
            type = Query.StatementType.UPDATE;
        } else {
            type = Query.StatementType.UNKNOWN;
        }
        return type;
    }
}