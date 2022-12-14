import org.eclipse.paho.client.mqttv3.*;
import redis.clients.jedis.Jedis;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MeteoServer {
    public static void main(String[] args) throws MqttException {
        int maxThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(maxThreads);
        for (int i = 0; i < maxThreads; i++) {
            executor.execute(new MeteoStation(i));
        }
        String publisherId = UUID.randomUUID().toString();
        try (MqttClient publisher = new MqttClient("tcp://54.166.107.43:1883", publisherId)) {
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            options.setConnectionTimeout(10);
            publisher.connect(options);

            publisher.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable throwable) {
                    System.out.println("Connection to Solace broker lost! " + throwable.getMessage());
                }

                @Override
                public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {
                    String idMeteo = topic.split("/")[3];
                    String KeyHash = String.format("FLAVIUS:LASTMEASUREMENT:%s", idMeteo);
                    String KeyList = String.format("FLAVIUS:TEMPERATURES:%s", idMeteo);
                    String keyAlert = String.format("FLAVIUS:ALERTS");
                    String[] splitMessage = new String(mqttMessage.getPayload()).split("#");
                    String dateTime = splitMessage[0];
                    dateTime += " " + splitMessage[1];
                    String temperatura = splitMessage[2];
                    try (Jedis jedis = new Jedis("54.166.107.43", 6000)) {
                        jedis.del(KeyHash);
                        jedis.hset(KeyHash, dateTime, temperatura);
                        jedis.rpush(KeyList, temperatura);
                        if (Integer.parseInt(temperatura) > 30 || Integer.parseInt(temperatura) < 0) {
                            jedis.rpush(keyAlert, String.format("Alerta por temperaturas extremas el %s a las %s en la estación %s", splitMessage[0], splitMessage[1], idMeteo));
                        }
                    }
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
                }
            });

            publisher.subscribe("/FLAVIUS/METEO/#", 0);
        } catch (MqttException e) {
            throw new RuntimeException(e);
        }
    }
}
