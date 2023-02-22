package source;

import io.confluent.avro.random.generator.Generator;
import java.io.File;
import java.io.IOException;
import java.util.Random;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import org.json.JSONObject;

public class PayloadGenerator {
   Random random;
  Generator generator;
  public static void main(final String[] args) throws IOException {
    PayloadGenerator gen = new PayloadGenerator("");
    Object o = gen.get();
    GenericRecord rec = (GenericRecord)o;
    Schema schema = rec.getSchema();

    System.out.println(o.toString());
    JSONObject json = new JSONObject(o.toString()); // Convert text to object
    System.out.println(json.toString(4)); // Print it with specified indentation
    System.out.println("done");
  }

  public PayloadGenerator(String schemaPathStr) throws IOException { //Config config,
    this.random = new Random();
    this.generator = new Generator.Builder().random(random).generation(1).schemaFile(new File(schemaPathStr)).build();
  }
    public Object get() {
      final Object generatedObject = generator.generate();
      return generatedObject;
    }
  }
