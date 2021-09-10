package com.thinkmicroservices.nats.jetstream.client.controller;

import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.MessageInfo;
import io.nats.client.api.PublishAck;
import io.nats.client.api.ReplayPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import java.io.IOException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.format.annotation.DateTimeFormat.ISO;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * This class provides a REST endpoint to test our <b>NATS JetStream</b>
 * integration/
 *
 * @author cwoodward
 */
@RestController
@Slf4j
public class RESTController implements ConnectionListener {

    @Value("${nats.servers}")
    private String[] servers;

    private Connection connection;
    private JetStreamManagement jetStreamManagement;

    private static final String STREAM_NAME = "NOTIFICATIONS";

    private static final String CONSUMER_1 = "monitor1";

    private static final String CONSUMER_SUBJECT_1 = STREAM_NAME + "." + CONSUMER_1;

    private static final int SUBSCRIBER_FETCH_TIMEOUT_IN_SECONDS = 3;
    private static final int CONSUMER_1_DEFAULT_MESSAGE_FETCH_COUNT = 1;
    private static final int CONSUMER_2_DEFAULT_MESSAGE_FETCH_COUNT = 1;
    private static final int CONSUMER_2_DEFAULT_SEQUENCE_ID = 0;
    private static final int DEFAULT_ZONE_TIME_OFFSET = 1;

    private static final String CONSUME_BY_SEQUENCE = "ConsumeBySequence";
    private static final String CONSUME_BY_TIME = "ConsumeByTime";

    private static final String DEFAULT_MESSAGE = "default message";

    private DateTimeFormatter zdtFormatter = DateTimeFormatter.ISO_INSTANT;

    /**
     * Get the stream information
     */
    @GetMapping(value = "/stream-info")
    public StreamInfo getStreamInfo() throws IOException, JetStreamApiException {
        return jetStreamManagement.getStreamInfo(STREAM_NAME);
    }

    /**
     * Publishes a message to the stream
     *
     * @param message
     * @return
     * @throws IOException
     */
    @GetMapping(value = "/pub/{message}")
    public StreamInfo publishMessage(@PathVariable String message) throws IOException {

        /* if the user doesn't supply a message, use the default message */
        if (message == null) {
            message = DEFAULT_MESSAGE;
        }

        /* get a JetStream instance from the current nats connection*/
        JetStream js = connection.jetStream();

        try {
            /* publish the message to the stream */
            PublishAck pubAck = js.publish(CONSUMER_SUBJECT_1, message.getBytes());

            return getStreamInfo();

        } catch (JetStreamApiException ex) {
            log.error(ex.getMessage(), ex);
        }
        return null;
    }

    /**
     * Request <i>count</i> messages from the subscription/.
     *
     * @param count
     * @return
     */
    @GetMapping(value = {"/consumer"})
    public List<String> consumer1(@RequestParam("count") Integer count) {

        if (count == null) {
            count = CONSUMER_1_DEFAULT_MESSAGE_FETCH_COUNT;
        }

        try {
            /* get a JetStream instance */
            JetStream js = connection.jetStream();

            /* configure a Consumer */
            ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
                    .durable(CONSUMER_1)
                    .deliverSubject(CONSUMER_SUBJECT_1)
                    .build();

            /* create the pull options */
            PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                    .configuration(consumerConfig)
                    .build();

            /* create the subscription */
            JetStreamSubscription subscription = js.subscribe(CONSUMER_SUBJECT_1, pullOptions);

            /* fetch n messages from the subscription. Timeout after some period if no messages become available */
            List<Message> messages = subscription.fetch(count, Duration.ofSeconds(SUBSCRIBER_FETCH_TIMEOUT_IN_SECONDS));

            /*ACK all received messages */
            for (Message m : messages) {
                m.ack();

            }
            /* before returning the list of message, format them into 
             simple strings rather than the full (large) Message. Note
            that if you decide to return the actual Message payload you
            will need to set <i>spring.jackson.serialization.FAIL_ON_EMPTY_BEANS=false</i>
            in the <i>application.properties</i> file.
             */
            return formatMessages(messages);
        } catch (IOException | JetStreamApiException ex) {
            Logger.getLogger(RESTController.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    /**
     * create a consumer with the starting message aligned with the
     * <i>startSequenceId</i>. The consumer is configured to use
     * <i>DeliverPolicy.ByStartSequence</i>.
     *
     * @param startSequenceId
     * @param count
     * @return
     */
    @GetMapping(value = {"/consumeBySequence"})
    public List<String> consumer2BySequenceId(@RequestParam("startSequenceId") Integer startSequenceId,
            @RequestParam("count") Integer count) {

        if (count == null) {
            count = CONSUMER_2_DEFAULT_MESSAGE_FETCH_COUNT;
        }

        if (startSequenceId == null) {
            // get the current time and subtract some the default hour value
            startSequenceId = CONSUMER_2_DEFAULT_SEQUENCE_ID;
        }

        try {

            /* create a JetStream instance */
            JetStream js = connection.jetStream();

            /* configure the consumer to use the <i>DeliverPolicy.ByStartSequence</i>. */
            ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
                    .durable(CONSUME_BY_SEQUENCE)
                    .deliverPolicy(DeliverPolicy.ByStartSequence)
                    .startSequence(startSequenceId)
                    .replayPolicy(ReplayPolicy.Instant)
                    .build();

            /* configure the pull options */
            PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                    .configuration(consumerConfig)
                    .build();

            /* create the subscription */
            JetStreamSubscription subscription = js.subscribe(CONSUMER_SUBJECT_1, pullOptions);

            /* retrieve <i>count</i> messages from the subscription. Timeout if sufficient messages aren't available */
            List<Message> messages = subscription.fetch(count, Duration.ofSeconds(SUBSCRIBER_FETCH_TIMEOUT_IN_SECONDS));

            /* return format the messages*/
            return formatMessages(messages);

        } catch (IOException | JetStreamApiException ex) {
            Logger.getLogger(RESTController.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    /**
     * create a consumer that retrieves <i>count</i> messages starting at the
     * specified <i>startTime</i>. The consumer is configured to use the
     * <i>DeliverPolicy.ByStartTime</i>.
     *
     * @param count
     * @param startTime
     * @return
     */
    @GetMapping(value = {"/consumeByTime"})
    public List<String> consumer2ByTime(@RequestParam("count") Integer count,
            @RequestParam("startTime") @DateTimeFormat(iso = ISO.DATE_TIME) ZonedDateTime startTime) {

        if (count == null) {
            count = CONSUMER_1_DEFAULT_MESSAGE_FETCH_COUNT;
        }

        if (startTime == null) {
            // get the current time and subtract some the default hour value
            startTime = ZonedDateTime.now().minusHours(DEFAULT_ZONE_TIME_OFFSET);
        }

        try {

            /* create a JetStream instance */
            JetStream js = connection.jetStream();

            /* configure the consumer to use <i>DeliverPolicy.ByStartTime</i>.*/
            ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
                    .durable(CONSUME_BY_TIME)
                    .deliverPolicy(DeliverPolicy.ByStartTime)
                    .startTime(startTime)
                    .replayPolicy(ReplayPolicy.Instant)
                    .build();

            /* create the pull options */
            PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                    .configuration(consumerConfig)
                    .build();

            /* create the subscription */
            JetStreamSubscription subscription = js.subscribe(CONSUMER_SUBJECT_1, pullOptions);

            /* retrieve <i>count</i> message. */
            List<Message> messages = subscription.fetch(count, Duration.ofSeconds(SUBSCRIBER_FETCH_TIMEOUT_IN_SECONDS));

            /* return a list of formatted messages. */
            return formatMessages(messages);

        } catch (IOException | JetStreamApiException ex) {
            Logger.getLogger(RESTController.class.getName()).log(Level.SEVERE, null, ex);
        }

        return null;
    }

    /**
     * This method reduces the comprehensive NATS message into a simple string
     * containing the timestamp, and message.
     *
     * @param messages
     * @return
     */
    private List<String> formatMessages(List<Message> messages) {
        messages.forEach(m -> System.out.println(m.metaData() + "/" + (new String(m.getData()))));
        return messages.stream().map(
                m -> String.format("%s : <%s>",
                        m.metaData().timestamp().format(zdtFormatter),
                        new String(m.getData()))
        )
                .collect(Collectors.toList());

    }

    /**
     * Create a connection to the configured NATS servers.
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    private Connection getConnection() {
        if ((connection == null) || (connection.getStatus() == Connection.Status.DISCONNECTED)) {

            Options.Builder connectionBuilder = new Options.Builder().connectionListener(this);

            /* iterate over the array of servers and add them to the  connection builder.
             */
            for (String server : servers) {
                String natsServer = "nats://" + server;
                log.info("adding nats server:" + natsServer);
                connectionBuilder.server(natsServer).maxReconnects(-1);
            }

            try {
                connection = Nats.connect(connectionBuilder.build());
            } catch (IOException | InterruptedException ex) {
                log.error(ex.getMessage());
            }
        }
        log.info("return connection:" + connection);
        return connection;
    }

    /**
     * Listen for NATS connection events.
     *
     * @param cnctn
     * @param event
     */
    @Override
    public void connectionEvent(Connection cnctn, Events event) {
        log.info("Connection Event:" + event);

        switch (event) {

            case CONNECTED:
                log.info("CONNECTED to NATS!");
                break;
            case DISCONNECTED:
                log.warn("RECONNECTED to NATS!");
                try {
                    connection = null;
                    getConnection();
                } catch (Exception ex) {
                    log.error(ex.getMessage(), ex);

                }
                break;
            case RECONNECTED:
                log.info("RECONNECTED to NATS!");
                break;
            case RESUBSCRIBED:
                log.info("RESUBSCRIBED!");
                break;

        }

    }

    /**
     * perform basic setup after the controller has been created.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @PostConstruct
    void postConstruct() throws IOException, InterruptedException {
        try {
            log.info("REST controller postConstruct.");
            createStream();
            preLoadStreamMessages();

        } catch (JetStreamApiException ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    /**
     * Create the stream we will be using.
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws JetStreamApiException
     */
    private void createStream() throws IOException, InterruptedException, JetStreamApiException {
        log.info("creating stream");
        connection = getConnection();

        JetStream js = connection.jetStream();
        jetStreamManagement = connection.jetStreamManagement();

        /* if the stream already exists, delete it */
        StreamInfo streamInfo = null;

        try {
            streamInfo = jetStreamManagement.getStreamInfo(STREAM_NAME);
            if (streamInfo != null) {
                log.warn("Stream already exists....");
                deleteStream();
            }
        } catch (JetStreamApiException ex) {
            log.info("Stream does not exist");
        }

        log.info("creating stream");
        /* configure the stream */
        try {
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .name(STREAM_NAME)
                    .storageType(StorageType.Memory)
                    .subjects(CONSUMER_SUBJECT_1)
                    .build();

            /* create the stream */
            streamInfo = jetStreamManagement.addStream(streamConfig);
            log.info("Created Stream", streamInfo);

        } catch (JetStreamApiException jsapiEx) {
            log.error(jsapiEx.getMessage());
        }
    }

    /**
     * delete the stream.
     */
    private void deleteStream() {
        log.info("Destroying Stream-" + STREAM_NAME);
        try {
            connection = getConnection();
            JetStream js = connection.jetStream();
            jetStreamManagement = connection.jetStreamManagement();
            jetStreamManagement.deleteStream(STREAM_NAME);
        } catch (IOException | JetStreamApiException ex) {
            log.warn(ex.getMessage(), ex);
        }
    }

    /**
     * publish <i>n</i> message to prime the stream for the demo.
     *
     * @throws IOException
     */
    private void preLoadStreamMessages() throws IOException {
        for (int idx = 0; idx < 10; idx++) {
            publishMessage(String.format("pre-loaded message (%d)", idx));
        }
    }

    /**
     * perform cleanup before the controller is destroyed.
     */
    @PreDestroy

    private void cleanup() {
        log.info("Consumer and stream cleanup");

        deleteStream();
    }
}
