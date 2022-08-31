Spring Stream Helper (CardX Common Helper Library)
A common helper library for stream message.

//Usage

Dependency
 - Maven dependency
Add following xml tag into the <dependencies> section inside the project pom.xml file

<dependency>
  <groupId>com.cmd.kafka</groupId>
  <artifactId>cmd-kafka-helper</artifactId>
</dependency>

//Example
@ComponentScan({"com.cmd"})

public class SimpleStreamingMessage {

    public static void main(String[] args) {
        SpringApplication.run(SimpleStreamingMessage.class, args);
    }
}

//Then create sender or receiver by call factory method.


@Service

public class SomeService {

    @Value("${subscribe.topic.path}")
    private String topicToReceive;

    @Value("${publish.topic.path}")
    private String topicToSend;

    private final StreamReceiverFactory receiverFactory;
    private final StreamSender streamSender;

    public SomeService(final StreamReceiverFactory streamReceiverFactory,
                       final StreamSenderFactory streamSenderFactory) {
        // can provide kafka config in Map<String, Object>. 
        // if not create with default configuration.
        this.receiverFactory = streamReceiverFactory;
        this.streamSender = streamSenderFactory.createKafkaStreamSender();
    }

    @PostConstruct
    public void receiveMessage() {
        receiverFactory
                .createKafkaStreamSender(topicToReceive, ReceiveDataModel.class)
                .receive(this::processMessage);
    }

    private Mono<Void> processMessage(ReceiveDataModel message) {
        /* process message logic */
    }

    public Mono<Void> sendMessage(final SendDataModel message) {
        return sender.send(topicToSend, message);
    }

}






