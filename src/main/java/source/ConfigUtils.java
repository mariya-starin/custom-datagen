package source;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.SchemaParseException;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigUtils {
    private static final Logger log = LoggerFactory.getLogger(ConfigUtils.class);

    public static Schema getSchemaFromSchemaString(String schemaString) {
        Schema.Parser schemaParser = new Parser();
        Schema schema;
        try {
            schema = schemaParser.parse(schemaString);
        } catch (SchemaParseException e) {
            log.error("Unable to parse the provided schema", e);
            throw new ConfigException("Unable to parse the provided schema");
        }
        return schema;
    }

    public static Schema getSchemaFromSchemaFileName(String schemaFileName) {
        Schema.Parser schemaParser = new Parser();
        Schema schema;
        try (InputStream stream = new FileInputStream(schemaFileName)) {
            schema = schemaParser.parse(stream);
        } catch (FileNotFoundException e) {
            try {
                if (ConfigUtils.class.getClassLoader()
                        .getResource(schemaFileName) == null) {
                    throw new ConfigException("Unable to find the schema file");
                }
                schema = schemaParser.parse(
                        ConfigUtils.class.getClassLoader().getResourceAsStream(schemaFileName)
                );
            } catch (SchemaParseException | IOException i) {
                log.error("Unable to parse the provided schema", i);
                throw new ConfigException("Unable to parse the provided schema");
            }
        } catch (SchemaParseException | IOException e) {
            log.error("Unable to parse the provided schema", e);
            throw new ConfigException("Unable to parse the provided schema");
        }
        return schema;
    }
}