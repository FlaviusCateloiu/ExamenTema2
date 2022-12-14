import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.UUID;

public class MeteoStation implements Runnable {
    private int id;
    private MqttClient publisher;
    private MqttConnectOptions options;

    public MeteoStation(int id) throws MqttException {
        this.id = id;
        String publisherId = UUID.randomUUID().toString();
        publisher = new MqttClient("tcp://54.166.107.43:1883", publisherId);
        options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(10);
        publisher.connect(options);
    }

    @Override
    public void run() {
        Random random = new Random();
        String fecha, hora, temperatura, mensaje;
        String topic = String.format("/FLAVIUS/METEO/%s", this.id);
        while (true) {
            fecha = LocalDate.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy"));
            hora = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm"));
            temperatura = String.valueOf(random.nextInt(40 - 10) + 10);
            mensaje = String.format("%s#%s#%s", fecha, hora, temperatura);
            try {
                publisher.publish(topic, new MqttMessage(mensaje.getBytes()));
                Thread.sleep(5000);
            } catch (MqttException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
