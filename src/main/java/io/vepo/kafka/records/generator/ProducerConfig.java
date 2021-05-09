package io.vepo.kafka.records.generator;

public class ProducerConfig {
    private String id;
    private String topic;
    private SerializerType type;
    private int frequency;
    private String template;

    public String getId() {
	return id;
    }

    public void setId(String id) {
	this.id = id;
    }

    public String getTopic() {
	return topic;
    }

    public void setTopic(String topic) {
	this.topic = topic;
    }

    public SerializerType getType() {
	return type;
    }

    public void setType(SerializerType type) {
	this.type = type;
    }
    
    public int getFrequency() {
	return frequency;
    }
    
    public void setFrequency(int frequency) {
	this.frequency = frequency;
    }
    
    public String getTemplate() {
	return template;
    }
    
    public void setTemplate(String template) {
	this.template = template;
    }

    @Override
    public String toString() {
	return String.format("ProducerConfig [id=%s, topic=%s, type=%s, frequency=%d, template=%s]", id, topic, type, frequency, template);
    }

}
