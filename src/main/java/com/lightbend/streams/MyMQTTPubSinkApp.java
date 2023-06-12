package com.lightbend.streams;

import akka.Done;
import akka.NotUsed;
import akka.actor.CoordinatedShutdown;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.alpakka.mqtt.*;
import akka.stream.alpakka.mqtt.MqttMessage;
import akka.stream.alpakka.mqtt.javadsl.*;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import com.lightbend.actors.UserInitiatedShutdown;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;


public class MyMQTTPubSinkApp {
    private static final Logger log = LoggerFactory.getLogger(MyMQTTPubSinkApp.class);
    private static final Config appConfig = ConfigFactory.load();

    public static Behavior<NotUsed> rootBehavior() {
        return Behaviors.setup(context -> {
            final Duration runTimeout = Duration.ofSeconds(20);

            ActorSystem<?> system = context.getSystem();
            Logger log = context.getLog();

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

            Sink<MqttMessage, CompletionStage<Done>> mqttSink =
                    MqttSink.create(connectionSettings, MqttQoS.atLeastOnce());

            CompletionStage<Done> done = source
                    .map(msg -> {
                        log.info("Sending to broker -> {}", msg.payload().decodeString(UTF_8).toString());
                        return msg;
                    })
                    // left is Unused, right is CompletionStage<Done>
                    .toMat(mqttSink, Keep.right()).run(context.getSystem());

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