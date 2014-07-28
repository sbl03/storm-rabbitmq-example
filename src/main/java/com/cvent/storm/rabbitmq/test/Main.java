/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cvent.storm.rabbitmq.test;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.Scheme;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.rabbitmq.client.Channel;
import io.latent.storm.rabbitmq.Declarator;
import io.latent.storm.rabbitmq.Message;
import io.latent.storm.rabbitmq.RabbitMQProducer;
import io.latent.storm.rabbitmq.RabbitMQSpout;
import io.latent.storm.rabbitmq.config.ConnectionConfig;
import io.latent.storm.rabbitmq.config.ConsumerConfig;
import io.latent.storm.rabbitmq.config.ConsumerConfigBuilder;
import io.latent.storm.rabbitmq.config.ProducerConfig;
import io.latent.storm.rabbitmq.config.ProducerConfigBuilder;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author sliu
 */
public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    private static final String EXCHANGE_NAME = "storm-test-exchange";
    private static final String QUEUE_NAME = "storm-test-queue";
    private static final int NUM_MESSAGES = 10000;
    private static final boolean PRODUCE = false;

    public static void main(String[] args) {
        // Initialize everything
        Message msg = new Message("test message".getBytes());
        ConnectionConfig connConfig = new ConnectionConfig("localhost", "guest", "guest");
        Declarator decl = new CustomDeclarator(EXCHANGE_NAME, QUEUE_NAME);
        ProducerConfig pConfig = new ProducerConfigBuilder().connection(connConfig)
                .exchange(EXCHANGE_NAME)
                .contentEncoding("UTF-8")
                .contentType("text/plain")
                .build();
        ConsumerConfig spoutConfig = new ConsumerConfigBuilder().connection(connConfig)
                .queue(QUEUE_NAME)
                .prefetch(200)
                .requeueOnFail()
                .build();

        if (PRODUCE) {
            // Start producers
            RabbitMQProducer producer = new RabbitMQProducer(decl);
            producer.open(pConfig.asMap());

            LOGGER.info("Starting producers.");

            long start = System.currentTimeMillis();

            for (int i = 0; i < NUM_MESSAGES; i++) {
                producer.send(msg);
            }

            long delta = System.currentTimeMillis() - start;

            LOGGER.info("Took: {} ms @ {} msgs/s", delta, String.format("%.2f", NUM_MESSAGES / (delta / 1000.0)));
        }

        Scheme scheme = new Main.CustomScheme();
        IRichSpout spout = new RabbitMQSpout(scheme, decl);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("myspout", spout)
                .addConfigurations(spoutConfig.asMap())
                .setMaxSpoutPending(200);

        Config conf = new Config();
        conf.setDebug(true);

        // Start a local cluster
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.killTopology("test");
        cluster.shutdown();

        LOGGER.info("Done with everything.");
        //System.exit(0);
    }

    private static class CustomScheme implements Scheme {

        @Override
        public List<Object> deserialize(byte[] bytes) {
            try {
                return new Values(new String(bytes, "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Fields getOutputFields() {
            return new Fields("str");
        }

    }

    private static class CustomDeclarator implements Declarator {

        private final String exchange;
        private final String queue;
        private final String routingKey;

        public CustomDeclarator(String exchange, String queue) {
            this(exchange, queue, "");
        }

        public CustomDeclarator(String exchange, String queue, String routingKey) {
            this.exchange = exchange;
            this.queue = queue;
            this.routingKey = routingKey;
        }

        @Override
        public void execute(Channel channel) {
            // you're given a RabbitMQ Channel so you're free to wire up your exchange/queue bindings as you see fit
            try {
                Map<String, Object> args = new HashMap<>();
                channel.queueDeclare(queue, true, false, false, args);
                channel.exchangeDeclare(exchange, "topic", true);
                channel.queueBind(queue, exchange, routingKey);
            } catch (IOException e) {
                throw new RuntimeException("Error executing rabbitmq declarations.", e);
            }
        }
    }
}
