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

import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.io.kafka.KafkaRecord;

import org.joda.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DatumReader;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.avro.Schema;


public class ReadKafka {
    private static final Logger LOG = LoggerFactory.getLogger(ReadKafka.class);
    public interface ReadKafkaOptions extends PipelineOptions,StreamingOptions {
        @Default.String("bootstrap.kafka-console.us-east4.managedkafka.ggspandf.cloud.goog:9092")
        String getBootstrapServers();
        void setBootstrapServers(String bootstrapServers);

        @Default.String("RXOWNER.RXP_PRESCRIPTION")
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
        String avroSchema = "{ \"type\" : \"record\", \"name\" : \"RXP_PRESCRIPTION\", \"namespace\" : \"RXOWNER\", \"fields\" : [ { \"name\" : \"table\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"op_type\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"op_ts\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"current_ts\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"pos\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"before\", \"type\" : [ \"null\", { \"type\" : \"record\", \"name\" : \"columns\", \"fields\" : [ { \"name\" : \"PRESCRIPTION_ID\", \"type\" : [ \"null\", \"long\" ], \"default\" : null, \"primary_key\" : true }, { \"name\" : \"ACQUIRED_ID\", \"type\" : [ \"null\", \"double\" ], \"default\" : null }, { \"name\" : \"ORGANIZATION_ID\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"PATIENT_ADDRESS_ID\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"FACILITY_NUM\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"PRESCRIBER_ID\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"PATIENT_ID\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"CREATED_DATE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"CREATED_BY\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"LAST_UPDATED_DATE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"LAST_UPDATED_BY\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"PRODUCT_NUM\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"IMAGE_NUM\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"RX_NUMBER\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"RX_TYPE\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"SIG\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"IS_COMPOUND\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"NDC_PRESCRIBED_DRUG\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"RX_STATE\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"INACTIVATE_DATE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"INACTIVATION_REASON\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"PRESCRIPTION_DATE_WRITTEN\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"PRESCRIPTION_EXPIRATION_DATE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"PRN_INDICATOR\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"RENEW_REFILL_REQUEST_INDICATOR\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"ACQUIRED_INDICATOR\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"READY_FILL_ENROLLMENT_CD\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"CURRENT_FILL_NUMBER\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"PRESCRIBED_QUANTITY\", \"type\" : [ \"null\", \"double\" ], \"default\" : null }, { \"name\" : \"REFILL_QUANTITY\", \"type\" : [ \"null\", \"double\" ], \"default\" : null }, { \"name\" : \"PRESCRIBED_NUMBER_OF_REFILLS\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"REFILLS_REMAINING\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"QUANTITY_REMAINING\", \"type\" : [ \"null\", \"double\" ], \"default\" : null }, { \"name\" : \"NUMBER_OF_LABELS_TO_PRINT\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"PRINT_DRUG_NAME_ON_LABEL\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"ORIGINAL_PRESCRIBER_ID\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"FACILITY_ID\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"EXPANDED_SIG\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"LINKAGE_TYPE_CD\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"LINKED_TO_RX_NUMBER\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"GENERATED_TO_RX_NUMBER\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_IN_ORIGINAL_RX_NUMBER\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFERRED_IND\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"TRANSFER_IN_INDICATOR\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_IN_TYPE\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"TRANSFER_IN_FACILITY_NUMBER\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_IN_FACILITY_NAME\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_IN_FACILITY_ADDLINE1\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_IN_FACILITY_ADDLINE2\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_IN_FACILITY_CITY\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_IN_FACILITY_STATE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_IN_FACILITY_ZIP\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_IN_FACILITY_NABP_NUM\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"TRANSFER_IN_FACILITY_DEA_NUM\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_IN_FACILITY_PH_NUM\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_IN_PHARMACIST_NAME\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_IN_RPH_LICENSE_NUM\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_IN_DATE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_OUT_NEW_RX_NUMBER\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_OUT_INDICATOR\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_OUT_TYPE\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"TRANSFER_OUT_FACILITY_NUMBER\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_OUT_FACILITY_NAME\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_OUT_FACILITY_ADDLINE1\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_OUT_FACILITY_ADDLINE2\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_OUT_FACILITY_CITY\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_OUT_FACILITY_STATE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_OUT_FACILITY_ZIP\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_OUT_FACILITY_NABP_NUM\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"TRANSFER_OUT_FACILITY_DEA_NUM\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_OUT_FACILITY_PH_NUM\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_OUT_PHARMACIST_NAME\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_OUT_RPH_LICENSE_NUM\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_OUT_DATE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_OUT_FAX_DATE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"PRESCRIBER_ADDRESS_ID\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"GENERATED_FROM_RX_NUMBER\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"IS_DIRTY\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"IS_CURRENT_VERSION\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"RX_VERSION\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"LOCK_INDICATOR\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"GENERATED_FROM_FILE_BUY_IND\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"DRUG_SUBSTITUTED_IND\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFERRED_IN_NUM_OF_REFILL\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"TRANSFER_IN_FACILITY_COMP_CODE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_IN_FACILITY_LIC_NO\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"RX_LASTFILL_DATE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_OUT_FACILITY_COM_CODE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFER_OUT_FACILITY_LIC_NO\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"ORIGINAL_FILL_DATE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"LOCAL_PRES_DATE_WRITTEN\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"LOCAL_TRANSFER_IN_DATE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"LOCAL_TRANSFER_IN_DATE_WRITTEN\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"LOCAL_TRANSFER_OUT_DATE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"LOCAL_INACTIVATE_DATE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"SUPERVISING_PRESCRIBER_ID\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"ACQUIRED_RX_NUMBER\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"TRANSFERRED_IN_ORIG_FILLS\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"TRANSFERRED_IN_RX_DATE_WRITTEN\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"PRESCRIBER_ORDER_NUMBER\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"CONTROLLED_SUBSTANCE_ID_QUAL\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"CONTROLLED_SUBSTANCE_ID\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"RX_SERIAL_NUMBER\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"READY_FILL_ENROLLMENT_DATE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"SCHEDULED_FILL_DATE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"SCHEDULED_FILL_REASON\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"CREDENTIAL_COUNT\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"GEN_SUB_PERFORMED_CODE\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"DPS_NUMBER\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"DIAGNOSIS_CODE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"PROCEDURE_MODIFIER_CODE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"SRD_STATE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"RX_ORIGIN_CODE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"MAX_DAILY_DOSE_VALUE\", \"type\" : [ \"null\", \"double\" ], \"default\" : null }, { \"name\" : \"MAX_DAILY_DOSE_DOSAGE_UNIT\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"LINKED_FROM_RX_NUMBER\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"INACTIVATION_CODE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"PATIENT_WEIGHT_KG\", \"type\" : [ \"null\", \"double\" ], \"default\" : null }, { \"name\" : \"PAT_WEIGHT_CAPTURED_DATE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"DRUG_SEARCH_INDICATORS\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"DO_NOT_FILL_BEFORE_DATE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"IS_CONTROLLED_SUBSTANCE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"CALCULATED_REFILLS_REMAINING\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"TRANSFER_IN_CVS_PHARM_NAME\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"IS_EPCS_RX\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"SUPERVISING_PRES_ADDRESS_ID\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"WEB_RF_ENROLLMENT_UPDATED_VIA\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"CVS_CUSTOMER_ID\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"SIG_CODE_ORIGIN\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"LOCATION\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"ISSUE_CONTACT_NUMBER\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"IS_SIG_AVAILABLE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"IS_QUANTITY_AVAILABLE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"IS_DIAGNOSIS_CODE_AVAILABLE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"IS_DAW_AVAILABLE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"IS_HARD_COPY_CHECK_PERFORMED\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"ISP_INDICATOR\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"ISP_RX_PARTICIPATES_IND\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"ISP_METHOD_OF_TRANSITION\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"ISP_TRANSITION_DATE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"ISP_FILL_CODE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"ISP_TRACKER_CODE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"ENROLLMENT_DECLINE_COUNT\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"PATIENT_CONTROL_GROUP\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"LINKAGE_METHOD\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"RF_ENROLLMENT_CREDENTIALS\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"READY_FILL_EMPLOYEE_ID\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"READY_FILL_ENROLLMENT_REASON\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"RF_PATIENT_CONF_ATTEMPT_NO\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"RF_PATIENT_CONF_RESPONSE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"RF_PATIENT_CONF_TIMESTAMP\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"PPI_MESSAGE_ID\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"PRESCRIBER_ORDER_NUM\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"DRUG_DB_CODE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"DRUG_DB_CODE_QUALIFIER\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"LINK_DATE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"RX_SUB_TYPE\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"RX_REASSIGN_INDICATOR\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"SS_ENROLLMENT_IND\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"READYFILL_DUE_SS_CHANGE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"POS_RF_ENROLL_UNENROLLMENT_IND\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"SS_INELIGIBILITY_REASON\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"PROACTIVE_PROG_OUTCOME\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }, { \"name\" : \"ASSOCIATED_FROM_RX_NUMBER\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"IS_PRESCRIBER_NPI_AVAILABLE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"PROACTIVE_DISPOSITION_DATE\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"EXTENDED_SIG\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"EXPANDED_EXTENDED_SIG\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"HC_RESCAN_FLAG\", \"type\" : [ \"null\", \"string\" ], \"default\" : null }, { \"name\" : \"PROHIBITED_IND\", \"type\" : [ \"null\", \"string\" ], \"default\" : null } ] } ], \"default\" : null }, { \"name\" : \"after\", \"type\" : [ \"null\", \"columns\" ], \"default\" : null } ] }";

        // Parse the Avro schema
        Schema schema = new Schema.Parser().parse(avroSchema);

        pipeline
                /*
                 * Step #1: Read messages in from Kafka using {@link KafkaIO} and create a PCollection
                 * of KV<String, String>.
                 */
                .apply(
                        "Read From Kafka",
                        KafkaIO.<String, byte[]>read()
                                .withConsumerConfigUpdates(
                                        ImmutableMap.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                                                "security.protocol", "SASL_SSL",
                                                "sasl.mechanism", "OAUTHBEARER",
                                                "sasl.login.callback.handler.class","com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler",
                                                "sasl.jaas.config","org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"))
                                .withBootstrapServers(options.getBootstrapServers())
                                .withTopics(topicsList)
                                .withKeyDeserializer(
                                        StringDeserializer.class)
                                .withValueDeserializerAndCoder(ByteArrayDeserializer.class, ByteArrayCoder.of())
                                .withoutMetadata())
                // Deserialize Avro messages
                .apply(MapElements.via(new SimpleFunction<KV<String, byte[]>, String>() {
                    @Override
                    public String apply(KV<String, byte[]> input) {
                        byte[] avroBytes = input.getValue();
                        try {
                            DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(schema);
                            Decoder decoder = DecoderFactory.get().binaryDecoder(avroBytes, null);
                            GenericRecord record = datumReader.read(null, decoder);
                            return "Key: " + input.getKey() + ", Avro Record: " + record.toString();
                        } catch (Exception e) {
                            e.printStackTrace();
                            return "Failed to deserialize Avro message.";
                        }
                    }
                }))
                // Print the results
                .apply(MapElements.via(new SimpleFunction<String, Void>() {
                    @Override
                    public Void apply(String input) {
                        System.out.println(input);
                        return null;
                    }
                }));

        pipeline.run();

    }
}
