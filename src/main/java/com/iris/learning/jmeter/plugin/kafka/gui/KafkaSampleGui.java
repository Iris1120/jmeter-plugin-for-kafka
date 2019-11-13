package com.iris.learning.jmeter.plugin.kafka.gui;

import com.iris.learning.jmeter.plugin.kafka.sample.KafkaSample;
import org.apache.jmeter.gui.util.JSyntaxTextArea;
import org.apache.jmeter.gui.util.JTextScrollPane;
import org.apache.jmeter.gui.util.VerticalPanel;
import org.apache.jmeter.samplers.gui.AbstractSamplerGui;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jorphan.gui.JLabeledTextField;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import javax.swing.*;
import java.awt.*;

/**
 * @Author: Iris1120
 * @Description: https://github.com/XMeterSaaSService/kafka_jmeter
 * @Date: 2019/11/6
 */
public class KafkaSampleGui extends AbstractSamplerGui {
    private static final long serialVersionUID = -679491329541145875L;
    private static final Logger log = LoggingManager.getLoggerForClass();

    private final JLabeledTextField brokersField = new JLabeledTextField("Brokers");
    private final JLabeledTextField topicField = new JLabeledTextField("Topic");
    private final JLabeledTextField messageSerializerField = new JLabeledTextField("Message Serializer");
    private final JLabeledTextField keySerializerField = new JLabeledTextField("Key Serializer");
    private final JLabeledTextField keyField = new JLabeledTextField("Key");
    private final JSyntaxTextArea textMessage = new JSyntaxTextArea(10, 50);
    private final JLabel textArea = new JLabel("Message");
    private final JTextScrollPane textPanel = new JTextScrollPane(textMessage);

    public KafkaSampleGui(){
        super();
        this.init();
    }

    private void init(){
        log.info("Initializing the GUI.");
        setLayout(new BorderLayout());
        setBorder(makeBorder());

        add(makeTitlePanel(), BorderLayout.NORTH);
        JPanel mainPanel = new VerticalPanel();
        add(mainPanel, BorderLayout.CENTER);

        JPanel dPanel = new JPanel();
        dPanel.setLayout(new GridLayout(3,2));
        dPanel.add(brokersField);
        dPanel.add(topicField);
        dPanel.add(messageSerializerField);
        dPanel.add(keySerializerField);
        dPanel.add(keyField);

        JPanel controlPanel = new VerticalPanel();
        controlPanel.add(dPanel);
        controlPanel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createLineBorder(Color.gray), "Parameters"));
        mainPanel.add(controlPanel);

        JPanel contentPanel = new VerticalPanel();
        JPanel messageContentPanel = new JPanel(new BorderLayout());
        messageContentPanel.add(this.textArea, BorderLayout.NORTH);
        messageContentPanel.add(this.textPanel, BorderLayout.CENTER);
        contentPanel.add(messageContentPanel);
        contentPanel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createLineBorder(Color.gray), "Content"));
        mainPanel.add(contentPanel);

    }

    /**
     * sample's name
     */
    @Override
    public String getStaticLabel() {
        return "Kafka Sample";
    }

    @Override
    public void configure(TestElement element) {
        super.configure(element);
        KafkaSample sampler = (KafkaSample) element;
        this.brokersField.setText(sampler.getKafkaBrokers());
        this.topicField.setText(sampler.getKafkaTopic());
        this.keyField.setText(sampler.getKafkaKey());
        this.messageSerializerField.setText(sampler.getKafkaMessageSerializer());
        this.keySerializerField.setText(sampler.getKafkaKeySerializer());
        this.textMessage.setText(sampler.getKafkaMessage());
    }

    @Override
    public String getLabelResource() {
        return this.getClass().getSimpleName();
    }

    @Override
    public TestElement createTestElement() {
        KafkaSample sampler = new KafkaSample();
        this.setupSamplerProperties(sampler);
        return sampler;
    }

    private void setupSamplerProperties(KafkaSample sampler) {
        this.configureTestElement(sampler);
        sampler.setKafkaBrokers(this.brokersField.getText());
        sampler.setKafkaTopic(this.topicField.getText());
        sampler.setKafkaMessage(this.textMessage.getText());
        sampler.setKafkaKey(this.keyField.getText());
        sampler.setKafkaMessageSerializer(this.messageSerializerField.getText());
        sampler.setKafkaKeySerializer(this.keySerializerField.getText());
    }

    @Override
    public void modifyTestElement(TestElement testElement) {
        KafkaSample sampler = (KafkaSample) testElement;
        this.setupSamplerProperties(sampler);
    }

    @Override
    public void clearGui() {
        super.clearGui();
        this.brokersField.setText("kafka_server:9092");
        this.topicField.setText("jmeterTest");
        //this.keyField.setText("");
        this.messageSerializerField.setText("kafka.serializer.StringEncoder");
        this.keySerializerField.setText("kafka.serializer.StringEncoder");
        this.textMessage.setText("");
        this.keyField.setText("");
    }
}
