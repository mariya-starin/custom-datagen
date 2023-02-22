package source;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import io.confluent.avro.random.generator.Generator;
import io.confluent.connect.avro.AvroData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.AvroSchema;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatagenCustomSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(DatagenCustomSourceTask.class);
//    public static final String INCREMENT_FIELD = "increment";
    public static final String POSITION_FIELD = "position";
    public static final String SCHEMA_STR = "schema.string";
    private org.apache.avro.Schema avroSchema;
    private AvroData avroData;
//    Hashtable<Integer, Long> offsets = new Hashtable<Integer, Long>();

    Long offset;
    private String topicPrefix = null;
    int[] increments;
    Generator gen;
    Random random;

    @Override
    public String version() {
        return new DatagenCustomSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        topicPrefix = props.get(DatagenCustomSourceConnector.TOPIC_PREFIX_CONFIG);
//        final String incrementsString = props.get(DatagenCustomSourceConnector.INCREMENTS_CONFIG);
//        increments = Arrays.stream(incrementsString.split(",")).mapToInt(Integer::parseInt).toArray();
//        for (Integer increment : increments) {
//            offsets.put(increment, 0L);
//        }
        offset = 0L;
        String schemaStr = props.get(SCHEMA_STR);
        avroSchema = ConfigUtils.getSchemaFromSchemaString(schemaStr);
        this.random = new Random();
        this.gen = new Generator.Builder().random(random).generation(1).schema(avroSchema).build();
        avroData = new AvroData(1);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        final ArrayList<SourceRecord> records = new ArrayList<>();
//        for (Integer increment : increments) {
//            Long offset = offsets.get(increment);

//            if (offset == 0) {
//                final Map<String, Object> storedOffset = context.offsetStorageReader()
//                        .offset(Collections.singletonMap(INCREMENT_FIELD, increment));
//                if (storedOffset != null) {
//                    // we have a stored offset, let's use this one
//                    offset = (Long) storedOffset.get(POSITION_FIELD);
//                    log.info("We found an offset for increment {} value {}", increment, offset);
//                }
//            }

            final String topic = topicPrefix ;
            Object generatedObject = gen.generate();
            if (!(generatedObject instanceof GenericRecord)) {
                throw new RuntimeException(String.format(
                        "Expected Avro Random Generator to return instance of GenericRecord, found %s instead",
                        generatedObject.getClass().getName()
                ));
            }
            final GenericRecord randomAvroMessage = (GenericRecord) generatedObject;
            final org.apache.kafka.connect.data.Schema messageSchema = avroData.toConnectSchema(avroSchema);
            final Object messageValue = avroData.toConnectData(avroSchema, randomAvroMessage).value();

            records.add(
                    new SourceRecord(
                            null,                 // sourcePartition
                            offsetValue(++offset),                // sourceOffset
                            topic,                                // topic
                            null,                                 // partition
                            messageSchema,                        // valueSchema
                            messageValue                          // value
                    )
            );
//            offsets.put(increment, offset);
//        }

        synchronized (this) {
            this.wait(1000);
        }

        return records;
    }

    @Override
    public void stop() {
        log.trace("Stopping");
    }

//    private Map<String, Integer> offsetKey(int increment) {
//        return Collections.singletonMap(INCREMENT_FIELD, increment);
//    }

    private Map<String, Long> offsetValue(Long pos) {
        return Collections.singletonMap(POSITION_FIELD, pos);
    }

}