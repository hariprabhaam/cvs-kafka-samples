package org.cvs;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;

import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.joda.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ReadKafka {
    public interface ReadKafkaOptions extends PipelineOptions,StreamingOptions {
        @Default.String("bootstrap.kafka-console.us-east4.managedkafka.ggspandf.cloud.goog:9092")
        String getBootstrapServers();
        void setBootstrapServers(String bootstrapServers);

        @Default.String("mytopic")
        String getInputTopics();
        void setInputTopics(String inputTopics);

        @Default.String("AVRO")
        String getOutputFileFormat();
        void setOutputFileFormat(String outputFileFormat);

    }

    public static void main(String[] args) {
        ReadKafkaOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(ReadKafkaOptions.class);
        options.setStreaming(true);
        // Create the Pipeline with the specified options.
        Pipeline pipeline = Pipeline.create(options);

        List<String> topicsList = new ArrayList<>(Arrays.asList(options.getInputTopics().split(",")));

        PCollection<KV<String, String>> records =
                pipeline
                        /*
                         * Step #1: Read messages in from Kafka using {@link KafkaIO} and create a PCollection
                         * of KV<String, String>.
                         */
                        .apply(
                                "Read From Kafka",
                                KafkaIO.<String, String>read()
                                        .withConsumerConfigUpdates(
                                                ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                                                        "security.protocol", "SASL_SSL",
                                                        "sasl.mechanism", "OAUTHBEARER",
                                                        "sasl.login.callback.handler.class","com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler",
                                                        "sasl.jaas.config","org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"))
                                        .withBootstrapServers(options.getBootstrapServers())
                                        .withTopics(topicsList)
                                        .withKeyDeserializerAndCoder(
                                                StringDeserializer.class, NullableCoder.of(StringUtf8Coder.of()))
                                        .withValueDeserializerAndCoder(
                                                StringDeserializer.class, NullableCoder.of(StringUtf8Coder.of()))
                                        .withoutMetadata())
                        /* Step #2: Window the messages into minute intervals specified by the executor. */
                        .apply(
                                "Creating Window",
                                Window.into(
                                        FixedWindows.of(Duration.standardMinutes(1))));

        pipeline.run();

    }
}