package io.vepo.kafka.records.generator;

import java.util.List;

public class Config {
    private List<String> bootstrapServers;
    private String schemaRegistry;
    private List<ProducerConfig> producers;

    public List<String> getBootstrapServers() {
	return bootstrapServers;
    }

    public void setBootstrapServers(List<String> bootstrapServers) {
	this.bootstrapServers = bootstrapServers;
    }

    public String getSchemaRegistry() {
	return schemaRegistry;
    }

    public void setSchemaRegistry(String schemaRegistry) {
	this.schemaRegistry = schemaRegistry;
    }

    public List<ProducerConfig> getProducers() {
	return producers;
    }

    public void setProducers(List<ProducerConfig> producers) {
	this.producers = producers;
    }

    @Override
    public String toString() {
	return String.format("Config [bootstrapServers=%s, schemaRegistry=%s, producers=%s]", bootstrapServers,
		schemaRegistry, producers);
    }

}
