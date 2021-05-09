package io.vepo.kafka.records.generator;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.vepo.kafka.records.generator.templates.Template1;
import io.vepo.kafka.records.generator.templates.proto.Templates;

public class RecordGenerator {
    private static final AtomicBoolean running = new AtomicBoolean(true);
    private final static Logger logger = LoggerFactory.getLogger(RecordGenerator.class);

    public static void main(String[] args) throws FileNotFoundException, InterruptedException {
	Yaml yaml = new Yaml();
	var config = yaml.loadAs(new FileInputStream(Paths.get(".", "config.yaml").toFile()), Config.class);

	var executor = Executors.newFixedThreadPool(config.getProducers().size());

	config.getProducers().stream().map(producer -> new RecordGenerator(config, producer))
		.map(generator -> executor.submit(generator::start)).collect(Collectors.toList());

	Runtime.getRuntime().addShutdownHook(new Thread() {
	    @Override
	    public void run() {
		running.set(false);
		try {
		    executor.awaitTermination(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
		    Thread.currentThread().interrupt();
		}
	    }
	});

	while (!executor.awaitTermination(10L, TimeUnit.MINUTES)) {
	    System.out.println("Still running...");
	}
    }

    private Config config;

    private ProducerConfig producerConfig;

    public RecordGenerator(Config config, ProducerConfig producerConfig) {
	this.config = config;
	this.producerConfig = producerConfig;
    }

    private Properties configs() {
	var configs = new Properties();
	configs.put(BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
	configs.put(CLIENT_ID_CONFIG, producerConfig.getId());
	configs.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
	switch (producerConfig.getType()) {
	case AVRO:
	    configs.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
	    break;
	case PROTOBUF:
	    configs.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
	    break;
	case JSON:
	    configs.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class);
	    break;
	default:
	    throw new IllegalArgumentException("Unexpected value: " + producerConfig.getType());
	}
	configs.put(SCHEMA_REGISTRY_URL_CONFIG, config.getSchemaRegistry());
	return configs;
    }

    private void start() {
	try (var producer = new KafkaProducer<String, Object>(configs())) {
	    int index = 0;
	    while (running.get()) {
		logger.info("Sending message: {}", index++);
		try {
		    var sleepTime = Duration.ofSeconds(1).dividedBy(this.producerConfig.getFrequency());
		    var record = new ProducerRecord<String, Object>(producerConfig.getTopic(),
			    Integer.toHexString(index),
			    getObject(index, producerConfig.getType(), producerConfig.getTemplate()));
		    producer.send(record);
		    Thread.sleep(sleepTime.toMillis());
		} catch (InterruptedException e) {
		    Thread.currentThread().interrupt();
		}
	    }
	}
    }

    private Map<String, String> contentCache = new HashMap<>();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private Object getObject(int index, SerializerType type, String template) {
	try {
	    var templateContent = contentCache.computeIfAbsent(template, path -> {
		try {
		    return new String(Files.readAllBytes(Paths.get(path)));
		} catch (IOException e) {
		    logger.error("Error!", e);
		    throw new RuntimeException(e);
		}
	    });
	    if (template.endsWith("template-1.json")) {
		var json = objectMapper
			.readTree(templateContent.replaceAll(Pattern.quote("$index"), Integer.toString(index)));
		if (type == SerializerType.PROTOBUF) {
		    return Templates.TemplateProto1.newBuilder().setId(json.get("id").asLong())
			    .setEmail(json.get("email").asText()).setUsername(json.get("username").asText()).build();
		} else if (type == SerializerType.AVRO) {
		    Schema.Parser parser = new Schema.Parser();
		    Schema schema = parser
			    .parse(RecordGenerator.class.getResourceAsStream("/avro-schemas/template-1.json"));
		    GenericRecord record = new GenericData.Record(schema);
		    record.put("id", json.get("id").asLong());
		    record.put("username", json.get("username").asText());
		    record.put("email", json.get("email").asText());
		    return record;
		} else {
		    return new Template1(json.get("id").asLong(), json.get("username").asText(),
			    json.get("email").asText());
		}
	    }
	    throw new UnsupportedOperationException("No template for file: " + template);
	} catch (RuntimeException | IOException e) {
	    logger.error("Error!", e);
	    throw new RuntimeException(e);
	}
    }
}