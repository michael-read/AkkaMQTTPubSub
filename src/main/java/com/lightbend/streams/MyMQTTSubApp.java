package com.lightbend.streams;

import akka.Done;
import akka.NotUsed;
import akka.actor.CoordinatedShutdown;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import akka.stream.alpakka.mqtt.MqttConnectionSettings;
import akka.stream.alpakka.mqtt.MqttMessage;
import akka.stream.alpakka.mqtt.MqttQoS;
import akka.stream.alpakka.mqtt.MqttSubscriptions;
import akka.stream.alpakka.mqtt.javadsl.MqttSource;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.lightbend.actors.UserInitiatedShutdown;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

import static java.util.concurrent.TimeUnit.SECONDS;


public class MyMQTTSubApp {
    private static final Logger log = LoggerFactory.getLogger(MyMQTTSubApp.class);
    private static final Config appConfig = ConfigFactory.load();

    public static Behavior<NotUsed> rootBehavior() {
        return Behaviors.setup(context -> {
            final Duration runTimeout = Duration.ofMinutes(5);

            ActorSystem<?> system = context.getSystem();

            final MqttConnectionSettings connectionSettings =
                    MqttConnectionSettings.create(
                            "tcp://localhost:1883", "java-client-sub", new MemoryPersistence());

            final int bufferSize = 8;

            MqttSubscriptions subscriptions =
                    MqttSubscriptions.create("flow-test/test", MqttQoS.atLeastOnce());


            Source<MqttMessage, CompletionStage<Done>> mqttSource =
                    MqttSource.atMostOnce(
                            connectionSettings, subscriptions, bufferSize);

            CompletionStage<Done> done = mqttSource
                    .runWith(Sink.foreach(msg -> {
                        log.info("Recevied from broker -> {}", msg.payload().decodeString(Charset.defaultCharset()).toString());
                    }), context.getSystem());
            Done d = done.toCompletableFuture().get(runTimeout.getSeconds(), SECONDS);

            // shutdown
            log.info("requesting coordinated shutdown.");
            CoordinatedShutdown.get(system).runAll(new UserInitiatedShutdown());

            return Behaviors.empty();
        });
    }

    public static void main(String[] args) {
        ActorSystem<NotUsed> system = ActorSystem.create(rootBehavior(), "mqttSubApp", appConfig);
        system.getWhenTerminated();
    }
}
