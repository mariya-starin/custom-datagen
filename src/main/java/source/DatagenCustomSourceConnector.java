package source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.utils.AppInfoParser;


public class DatagenCustomSourceConnector extends SourceConnector {
    private static Logger log = LoggerFactory.getLogger(DatagenCustomSourceConnector.class);
    public static final String TOPIC_PREFIX_CONFIG = "topic.prefix";
    public static final String DEFAULT_TOPIC_PREFIX = "increment_";
    public static final String INCREMENTS_CONFIG = "increments";
    public static final String SCHEMA_STR = "schema.string";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TOPIC_PREFIX_CONFIG, Type.STRING, DEFAULT_TOPIC_PREFIX, Importance.LOW, "Prefix for topics to publish data to")
            .define(INCREMENTS_CONFIG, Type.LIST, Importance.HIGH, "A list of increments for the sequences")
            .define(SCHEMA_STR, Type.STRING, Importance.HIGH, "Schema for generating records");

    private List<String> increments;
    private String topicPrefix;

    private String schemaStr;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {

        log.info("STARTING DatagenCustomSourceConnector");
        final AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
        increments = parsedConfig.getList(INCREMENTS_CONFIG);
        schemaStr = parsedConfig.getString(SCHEMA_STR);
        // config validation: all the increments need to be integers
        try {
            for (String s : increments) {
                Integer.parseInt(s);
            }
        } catch (java.lang.NumberFormatException e) {
            throw new ConfigException("'increments' must be a collection of integers");
        }

        topicPrefix = parsedConfig.getString(TOPIC_PREFIX_CONFIG);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return DatagenCustomSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {

        final int numGroups = Math.min(increments.size(), maxTasks);
        final List<List<String>> incrementsGrouped = ConnectorUtils.groupPartitions(increments, numGroups);
        final List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (List<String> taskIncrements : incrementsGrouped) {
            final Map<String, String> taskProps = new HashMap<>();
            taskProps.put(TOPIC_PREFIX_CONFIG, topicPrefix);
            taskProps.put(INCREMENTS_CONFIG, String.join(",", taskIncrements));
            taskProps.put(SCHEMA_STR,schemaStr);
            taskConfigs.add(taskProps);
            log.info("Task config: (prefix: {}, increments {})", topicPrefix, String.join(",", taskIncrements));
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        log.info("STOPPING DatagenCustommentSourceConnector");
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}

