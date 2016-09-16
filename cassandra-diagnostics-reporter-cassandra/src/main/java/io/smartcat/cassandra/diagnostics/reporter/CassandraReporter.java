package io.smartcat.cassandra.diagnostics.reporter;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;

import io.smartcat.cassandra.diagnostics.Measurement;

/**
 * A Cassandra based {@link Reporter} implementation. Query reports are sent to cassandra.
 */
public class CassandraReporter extends Reporter {

    /**
     * Class logger.
     */
    private static final Logger logger = LoggerFactory.getLogger(CassandraReporter.class);

    private static final String CONTACT_POINT_PROP = "contactPoint";

    private static final String PORT_PROP = "port";

    private static final String DEFAULT_PORT = "9042";

    private static final String KEYSPACE_NAME_PROP = "keyspace";

    private static final String DEFAULT_KEYSPACE_NAME = "cassandra_dianostics";

    private static final String CONSISTENCY_LEVEL_PROP = "consistencyLevel";

    private static final String DEFAULT_CONSISTENCY_LEVEL = "ONE";

    private static final SimpleDateFormat SIMPLE_DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");

    private PreparedStatement insertMeasurementStatement;

    private static Session session;

    private ConsistencyLevel consistencyLevel;

    /**
     * Constructor.
     *
     * @param configuration Reporter configuration
     */
    public CassandraReporter(ReporterConfiguration configuration) {
        super(configuration);

        if (!configuration.options.containsKey(CONTACT_POINT_PROP)) {
            logger.warn("Not properly configured. Missing cassandra contact point. Aborting initialization.");
            return;
        }

        String contactPoint = configuration.options.get(CONTACT_POINT_PROP);
        int port = Integer.parseInt(configuration.getDefaultOption(PORT_PROP, DEFAULT_PORT));
        String keyspace = configuration.getDefaultOption(KEYSPACE_NAME_PROP, DEFAULT_KEYSPACE_NAME);
        consistencyLevel = ConsistencyLevel
                .valueOf(configuration.getDefaultOption(CONSISTENCY_LEVEL_PROP, DEFAULT_CONSISTENCY_LEVEL));

        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(port).build();
        // session = cluster.connect();

        // createKeyspaceIfNotExists(keyspace);
        // createMeasurementTableIfNotExists();
        // insertMeasurementStatement = session.prepare("INSERT INTO " + keyspace
        // + ".measurements (name, host, date, timestamp, value, fields, tags) VALUES (?, ?, ?, ?, ?, ?, ?)");
    }

    private void createKeyspaceIfNotExists(final String keyspace) {
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " "
                + "WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
    }

    private void createMeasurementTableIfNotExists() {
        final String createMeasurementsByHostTable = SchemaBuilder
                .createTable("measurements")
                .addPartitionKey("name", DataType.text())
                .addPartitionKey("host", DataType.text())
                .addPartitionKey("date", DataType.text())
                .addClusteringColumn("timestamp", DataType.timestamp())
                .addColumn("value", DataType.cdouble())
                .addColumn("fields", DataType.frozenMap(DataType.text(), DataType.text()))
                .addColumn("tags", DataType.frozenMap(DataType.text(), DataType.text()))
                .ifNotExists().withOptions()
                .comment("Measurement by host table.")
                .buildInternal();

        final Statement statement = new SimpleStatement(createMeasurementsByHostTable);
        statement.setConsistencyLevel(ConsistencyLevel.ALL);
        session.execute(statement);
    }

    @Override
    public void report(Measurement measurement) {
        if (session == null) {
            logger.warn("Cassandra session is not initialized");
            return;
        }

        long timestampInMillis = measurement.timeUnit().toMillis(measurement.time());

        Date date = new Date(timestampInMillis);
        String datePortion = SIMPLE_DATE_FORMATTER.format(date);

        logger.debug("Sending Query: {}", measurement.toString());
        try {
            session.executeAsync(
                    insertMeasurementStatement.bind(measurement.name(), "host", datePortion, date, measurement.value(),
                            measurement.fields(), measurement.tags()).setConsistencyLevel(consistencyLevel));
        } catch (Exception e) {
            logger.warn("Failed to send report to Cassandra", e);
        }
    }

}

