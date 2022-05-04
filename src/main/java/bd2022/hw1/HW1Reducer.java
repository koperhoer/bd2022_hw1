package bd2022.hw1;

import java.io.IOException;

import com.google.gson.Gson;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Класс-редьюсер.
public class HW1Reducer extends Reducer<Text, IntWritable, Text, Text> {
    // Можно использовать для хранения справочника температур распределённый кэш.
    // Но справочник совсем небольшой, а время доступа к кэшу велико.
    // Будем считать, что хранить справочник в коде выгоднее в нашей задаче.
    private String temps_Json = "[\n" +
            "    {\n" +
            "        \"temperature\": \"low\",\n" +
            "        \"start\": \"0\",\n" +
            "        \"end\": \"500000\"\n" +
            "    },\n" +
            "    {\n" +
            "        \"temperature\": \"medium\",\n" +
            "        \"start\": \"500000\",\n" +
            "        \"end\": \"1000000\"\n" +
            "    },\n" +
            "    {\n" +
            "        \"temperature\": \"high\",\n" +
            "        \"start\": \"1000000\",\n" +
            "        \"end\": \"999999999\"\n" +
            "    }\n" +
            "]";
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // Читаем из JSON в массив классов температур с помощью гугловой библиотеки.
        Gson gson = new Gson();
        Temperature[] temps = gson.fromJson(temps_Json, Temperature[].class);
        // Число нажатий храним в long.
        Long num_clicks = Long.valueOf(0);
        // Выполняем суммирование по ключу числа нажатий.
        while (values.iterator().hasNext()) {
            num_clicks += values.iterator().next().get();
        }
        // Если в справочнике нет подходящей температуры, в значение запишется None.
        String temperature_str = "None";
        // Если же есть, то запишется текстовое обозначение соответствующей температуры.
        for(Temperature t : temps)
            if(t.contains(num_clicks))
                temperature_str = t.temperature;
        // Запишем в значение итоговое число нажатий на элемент с конкретным логическим именем
        // и его "температуру", найденную в справочнике.
        context.write(key, new Text("clicks=" + num_clicks.toString() + ",heat=" + temperature_str));
    }
}