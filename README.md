# Akka (Alpakka) MQTT Pub / Sub

1. Start MQTT Broker w/ docker-compose
```
docker-compose -f docker-compose-mqtt.yml up
```
2. Run MyMQTTSubApp from IntelliJ (this stops in 5 minutes)
3. Run MyMQTTPubFlowApp from IntelliJ (this publishes 20 messages and stops)