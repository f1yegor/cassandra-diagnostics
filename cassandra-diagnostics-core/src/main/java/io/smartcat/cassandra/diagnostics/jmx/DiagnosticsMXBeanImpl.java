package io.smartcat.cassandra.diagnostics.jmx;

import io.smartcat.cassandra.diagnostics.config.Configuration;

/**
 * Diagnostics JMX MXBean.
 */
public class DiagnosticsMXBeanImpl implements DiagnosticsMXBean {

    /**
     * Module configuration.
     */
    private Configuration config;

    /**
     * Constructor.
     *
     * @param config configuration object
     */
    public DiagnosticsMXBeanImpl(Configuration config) {
        this.config = config;
    }

}
