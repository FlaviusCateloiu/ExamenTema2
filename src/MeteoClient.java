import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Scanner;
import java.util.Set;

public class MeteoClient {
    public static void main(String[] args) {
        String opcion, comando, id = "";
        Scanner sc = new Scanner(System.in);
        try (Jedis jedis = new Jedis("54.166.107.43", 6000)) {
            do {
                System.out.println("-".repeat(10) + "Menu" + "-".repeat(10));
                System.out.println("EXIT");
                System.out.println("LAST ID. Muestra las últimas medidas de la estación meteorológica con ese ID.");
                System.out.println("MAXTEMP ID. Muestra la temperatura más alta de la estación meteorológica con ese ID.");
                System.out.println("MAXTEMP ALL. Muestra la temperatura más alta del sistema (busca en todas las\n" +
                        "estaciones meteorológicas).");
                System.out.println("ALERTS. Muestra las alertas actuales y las elimina.");
                opcion = sc.nextLine();
                comando = opcion.split(" ")[0];
                if (!comando.equalsIgnoreCase("alerts")) {
                    if (!comando.equalsIgnoreCase("exit")) {
                        id = opcion.split(" ")[1];
                    }
                }
                switch (comando.toUpperCase()) {
                    case "LAST" -> {
                        Set<String> keysLast = jedis.hkeys(String.format("FLAVIUS:LASTMEASUREMENT:%s", id));
                        if (keysLast != null) {
                            for (String c : keysLast) {
                                System.out.println(c + ": " + jedis.hget(String.format("FLAVIUS:LASTMEASUREMENT:%s", id), c));
                            }
                        } else {
                            System.out.println("No hay tiempo anterior.");
                        }

                    }
                    case "MAXTEMP" -> {
                        if (!id.equalsIgnoreCase("all"))  {
                            Set<String> keysTempID = jedis.hkeys(String.format("FLAVIUS:TEMPERATURES:%s", id));
                            if (keysTempID != null) {
                                for (String c : keysTempID) {
                                    System.out.println(c + ": " + jedis.hget(String.format("FLAVIUS:TEMPERATURES:%s", id), c));
                                }
                            } else {
                                System.out.println("No hay temperaturas de esa estacion.");
                            }
                        }
                    }
                    case "ALERTS" -> {
                        List<String> alerts = jedis.lrange("FLAVIUS:ALERTS", 0, -1);
                        if (alerts != null) {
                            for (String a : alerts) {
                                System.out.println(a);
                            }
                            jedis.del("FLAVIUS:ALERTS");
                        } else {
                            System.out.println("No hay alertas.");
                        }

                    }
                    case "EXIT" -> System.out.print("");
                }
            } while(!opcion.equalsIgnoreCase("exit"));
        }
    }
}
