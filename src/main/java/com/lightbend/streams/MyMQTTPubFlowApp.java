package com.lightbend.streams;

import akka.Done;
import akka.NotUsed;
import akka.actor.CoordinatedShutdown;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.alpakka.mqttv5.MqttConnectionSettings;
import akka.stream.alpakka.mqttv5.MqttMessage;
import akka.stream.alpakka.mqttv5.MqttQoS;
import akka.stream.alpakka.mqttv5.MqttSubscriptions;
import akka.stream.alpakka.mqttv5.javadsl.MqttFlow;
import akka.stream.alpakka.mqttv5.javadsl.MqttMessageWithAck;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import com.lightbend.actors.UserInitiatedShutdown;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

import static java.util.concurrent.TimeUnit.SECONDS;


public class MyMQTTPubFlowApp {
    private static final Logger log = LoggerFactory.getLogger(MyMQTTPubFlowApp.class);
    private static final Config appConfig = ConfigFactory.load();

    public static Behavior<NotUsed> rootBehavior() {
        return Behaviors.setup(context -> {
            final Duration runTimeout = Duration.ofSeconds(20);

            ActorSystem<?> system = context.getSystem();

            final MqttConnectionSettings connectionSettings =
                    MqttConnectionSettings.create(
                            "tcp://localhost:1883", "java-client-pub", new MemoryPersistence());

            final int bufferSize = 8;

            Source<MqttMessage, NotUsed> source = Source.range(1, 20)
                    .map(i -> {
                        return MqttMessage.create("flow-test/test", ByteString.fromString(i.toString()))
                                .withQos(MqttQoS.atLeastOnce())
                                .withRetained(true);
                    });

            MqttSubscriptions subscriptions =
                    MqttSubscriptions.create("flow-test/test", MqttQoS.atLeastOnce());

            Flow<MqttMessage, MqttMessageWithAck, CompletionStage<Done>> flow = MqttFlow.atLeastOnce(
                    connectionSettings, subscriptions, bufferSize, MqttQoS.atLeastOnce()
            );

            CompletionStage<Done> done = source
                    .map(msg -> {
                        log.info("Sending to broker -> {}", msg.payload().decodeString(Charset.defaultCharset()).toString());
                        return msg;
                    })
                    .via(flow)
                    .mapAsync(1, elem -> elem.ack())
                    .runWith(Sink.ignore(), context.getSystem());

            Done d = done.toCompletableFuture().get(runTimeout.getSeconds(), SECONDS);

            // shutdown
            log.info("requesting coordinated shutdown.");
            CoordinatedShutdown.get(system).runAll(new UserInitiatedShutdown());

            return Behaviors.empty();
        });
    }

    public static void main(String[] args) {
        ActorSystem<NotUsed> system = ActorSystem.create(rootBehavior(), "mqttPubApp", appConfig);
        system.getWhenTerminated();
    }
}