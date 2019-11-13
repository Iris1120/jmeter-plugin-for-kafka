package com.iris.learning.jmeter.plugin.kafka.sample;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import java.text.MessageFormat;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: Iris1120
 * @Description: https://github.com/XMeterSaaSService/kafka_jmeter
 * @Date: 2019/11/6
 */
public class KafkaSample extends AbstractSampler implements TestStateListener {
    private static final long serialVersionUID = -679491329541145875L;
    private static final String KAFKA_BROKERS = "kafka.brokers";
    private static final String KAFKA_TOPIC = "kafka.topic";
    private static final String KAFKA_KEY = "kafka.key";
    private static final String KAFKA_MESSAGE = "kafka.message";
    private static final String KAFKA_MESSAGE_SERIALIZER = "kafka.message.serializer";
    private static final String KAFKA_KEY_SERIALIZER = "kafka.key.serializer";
    private static ConcurrentHashMap<String, Producer<String, String>> producers = new ConcurrentHashMap<String, Producer<String, String>>();
    private static final Logger log = LoggingManager.getLoggerForClass();

    public KafkaSample() {
        setName("Kafka Sample");
    }

    @Override
    public SampleResult sample(Entry entry) {
        SampleResult result = new SampleResult();
        result.setSampleLabel(getName());
        try {
            result.sampleStart();
            Producer<String, String> producer = getProducer();
            KeyedMessage<String, String> msg = new KeyedMessage<String, String>(getKafkaTopic(),getKafkaKey(), getKafkaMessage());
            producer.send(msg);
            result.sampleEnd();
            result.setSuccessful(true);
            result.setResponseCodeOK();
        } catch (Exception e) {
            result.sampleEnd();
            result.setSuccessful(false);
            result.setResponseMessage("Exception: " + e);
            // get stack trace as a String to return as document data
            java.io.StringWriter stringWriter = new java.io.StringWriter();
            e.printStackTrace(new java.io.PrintWriter(stringWriter));
            result.setResponseData(stringWriter.toString(), null);
            result.setDataType(org.apache.jmeter.samplers.SampleResult.TEXT);
            result.setResponseCode("FAILED");
        }
        return result;
    }

    private Producer<String, String> getProducer() {
        String threadGrpName = getThreadName();
        Producer<String, String> producer = producers.get(threadGrpName);
        if (producer == null) {
            log.info(MessageFormat.format("Cannot find the producer for {0}, going to create a new producer.", threadGrpName));
            Properties props = new Properties();
            props.put("metadata.broker.list", getKafkaBrokers());
            props.put("serializer.class", getKafkaMessageSerializer() );
            props.put("key.serializer.class", getKafkaKeySerializer());
            props.put("request.required.acks", "1");
            ProducerConfig config = new ProducerConfig(props);
            producer = new Producer<String, String>(config);
            producers.put(threadGrpName, producer);
        }
        return producer;
    }

    public String getKafkaBrokers() {
        return getPropertyAsString(KAFKA_BROKERS);
    }

    public void setKafkaBrokers(String kafkaBrokers) {
        setProperty(KAFKA_BROKERS, kafkaBrokers);
    }

    public String getKafkaTopic() {
        return getPropertyAsString(KAFKA_TOPIC);
    }

    public void setKafkaTopic(String kafkaTopic) {
        setProperty(KAFKA_TOPIC, kafkaTopic );
    }

    public String getKafkaKey() {
        return getPropertyAsString(KAFKA_KEY);
    }

    public void setKafkaKey(String kafkaKey) {
        setProperty(KAFKA_KEY, kafkaKey);
    }

    public String getKafkaMessage() {
        return getPropertyAsString(KAFKA_MESSAGE);
    }

    public void setKafkaMessage(String kafkaMessage) {
        setProperty(KAFKA_MESSAGE, kafkaMessage);
    }

    public String getKafkaMessageSerializer() {
        return getPropertyAsString(KAFKA_MESSAGE_SERIALIZER);

    }

    public void setKafkaMessageSerializer(String kafkaMessageSerializer) {
        setProperty(KAFKA_MESSAGE_SERIALIZER, kafkaMessageSerializer);
    }

    public String getKafkaKeySerializer() {
        return getPropertyAsString(KAFKA_KEY_SERIALIZER);
    }

    public void setKafkaKeySerializer(String kafkaKeySerializer) {
        setProperty(KAFKA_KEY_SERIALIZER, kafkaKeySerializer);
    }


    @Override
    public void testStarted() {

    }

    @Override
    public void testStarted(String s) {

    }

    @Override
    public void testEnded() {
        this.testEnded("local");
    }

    @Override
    public void testEnded(String s) {

    }
}
