package bd2022.hw1;

import com.google.gson.Gson;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    // Можно использовать для хранения справочника элементов экрана распределённый кэш.
    // Но справочник совсем небольшой, а время доступа к кэшу велико.
    // Будем считать, что хранить справочник в коде выгоднее в нашей задаче.
    private final String read_Json = "[\n" +
            "    {\n" +
            "        \"type\": \"button\",\n" +
            "        \"logic_name\": \"main_page\",\n" +
            "        \"startX\": \"50\",\n" +
            "        \"startY\": \"25\",\n" +
            "        \"endX\": \"150\",\n" +
            "        \"endY\": \"75\"\n" +
            "    },\n" +
            "    {\n" +
            "        \"type\": \"button\",\n" +
            "        \"logic_name\": \"login_signup\",\n" +
            "        \"startX\": \"1800\",\n" +
            "        \"startY\": \"25\",\n" +
            "        \"endX\": \"1900\",\n" +
            "        \"endY\": \"75\"\n" +
            "    },\n" +
            "    {\n" +
            "        \"type\": \"button\",\n" +
            "        \"logic_name\": \"about_us\",\n" +
            "        \"startX\": \"1700\",\n" +
            "        \"startY\": \"25\",\n" +
            "        \"endX\": \"1775\",\n" +
            "        \"endY\": \"75\"\n" +
            "    },\n" +
            "    {\n" +
            "        \"type\": \"button\",\n" +
            "        \"logic_name\": \"new_post\",\n" +
            "        \"startX\": \"1700\",\n" +
            "        \"startY\": \"900\",\n" +
            "        \"endX\": \"1800\",\n" +
            "        \"endY\": \"950\"\n" +
            "    },\n" +
            "    {\n" +
            "        \"type\": \"button\",\n" +
            "        \"logic_name\": \"our_partners\",\n" +
            "        \"startX\": \"50\",\n" +
            "        \"startY\": \"900\",\n" +
            "        \"endX\": \"150\",\n" +
            "        \"endY\": \"950\"\n" +
            "    },\n" +
            "    {\n" +
            "        \"type\": \"ad_banner\",\n" +
            "        \"logic_name\": \"quick_loans\",\n" +
            "        \"startX\": \"50\",\n" +
            "        \"startY\": \"150\",\n" +
            "        \"endX\": \"200\",\n" +
            "        \"endY\": \"300\"\n" +
            "    },\n" +
            "    {\n" +
            "        \"type\": \"ad_banner\",\n" +
            "        \"logic_name\": \"expensive_flat\",\n" +
            "        \"startX\": \"50\",\n" +
            "        \"startY\": \"350\",\n" +
            "        \"endX\": \"200\",\n" +
            "        \"endY\": \"500\"\n" +
            "    },\n" +
            "    {\n" +
            "        \"type\": \"ad_banner\",\n" +
            "        \"logic_name\": \"XXX_site\",\n" +
            "        \"startX\": \"50\",\n" +
            "        \"startY\": \"550\",\n" +
            "        \"endX\": \"200\",\n" +
            "        \"endY\": \"700\"\n" +
            "    },\n" +
            "    {\n" +
            "        \"type\": \"ad_banner\",\n" +
            "        \"logic_name\": \"tabloid_rubbish\",\n" +
            "        \"startX\": \"300\",\n" +
            "        \"startY\": \"900\",\n" +
            "        \"endX\": \"500\",\n" +
            "        \"endY\": \"950\"\n" +
            "    },\n" +
            "    {\n" +
            "        \"type\": \"ad_banner\",\n" +
            "        \"logic_name\": \"ancient_workout\",\n" +
            "        \"startX\": \"1500\",\n" +
            "        \"startY\": \"700\",\n" +
            "        \"endX\": \"1600\",\n" +
            "        \"endY\": \"850\"\n" +
            "    },\n" +
            "    {\n" +
            "        \"type\": \"decoration\",\n" +
            "        \"logic_name\": \"long_logo\",\n" +
            "        \"startX\": \"200\",\n" +
            "        \"startY\": \"0\",\n" +
            "        \"endX\": \"1650\",\n" +
            "        \"endY\": \"75\"\n" +
            "    },\n" +
            "    {\n" +
            "        \"type\": \"decoration\",\n" +
            "        \"logic_name\": \"subdued_logo\",\n" +
            "        \"startX\": \"200\",\n" +
            "        \"startY\": \"950\",\n" +
            "        \"endX\": \"1650\",\n" +
            "        \"endY\": \"1080\"\n" +
            "    },\n" +
            "    {\n" +
            "        \"type\": \"navigation\",\n" +
            "        \"logic_name\": \"all_topics\",\n" +
            "        \"startX\": \"400\",\n" +
            "        \"startY\": \"150\",\n" +
            "        \"endX\": \"600\",\n" +
            "        \"endY\": \"750\"\n" +
            "    },\n" +
            "    {\n" +
            "        \"type\": \"navigation\",\n" +
            "        \"logic_name\": \"all_posts\",\n" +
            "        \"startX\": \"650\",\n" +
            "        \"startY\": \"150\",\n" +
            "        \"endX\": \"850\",\n" +
            "        \"endY\": \"750\"\n" +
            "    },\n" +
            "    {\n" +
            "        \"type\": \"navigation\",\n" +
            "        \"logic_name\": \"categories\",\n" +
            "        \"startX\": \"900\",\n" +
            "        \"startY\": \"150\",\n" +
            "        \"endX\": \"1100\",\n" +
            "        \"endY\": \"750\"\n" +
            "    },\n" +
            "    {\n" +
            "        \"type\": \"input_field\",\n" +
            "        \"logic_name\": \"search\",\n" +
            "        \"startX\": \"400\",\n" +
            "        \"startY\": \"800\",\n" +
            "        \"endX\": \"475\",\n" +
            "        \"endY\": \"850\"\n" +
            "    },\n" +
            "    {\n" +
            "        \"type\": \"input_field\",\n" +
            "        \"logic_name\": \"search_with_google\",\n" +
            "        \"startX\": \"500\",\n" +
            "        \"startY\": \"800\",\n" +
            "        \"endX\": \"575\",\n" +
            "        \"endY\": \"850\"\n" +
            "    }\n" +
            "]";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws RuntimeException {
        // Читаем информацию об элементах экрана из JSON гугловой библиотекой.
        Gson gson = new Gson();
        ScreenElement[] screen_elements = gson.fromJson(read_Json, ScreenElement[].class);
        // Берём поступившее на вход нажатие и смотрим, попадает ли оно на какой-либо элемент.
        try {
            Click mapper_click = new Click(value.toString());
            for(ScreenElement se : screen_elements) {
                if (se.containsClick(mapper_click)) {
                    // Если попадает, выдаём логическое имя этого элемента и единицу.
                    context.write(new Text(se.getLogicName()), new IntWritable(1));
                    return;
                }
            }
            // Если же нет, инкрементируем счётчик нажатий на пустоту.
            context.getCounter(CounterType.CLICKMISS).increment(1);
            return;
        } catch (Exception e) {
            // Исключения отлавливаем и пробрасываем вверх.
            throw new RuntimeException(e);
        }
    }
}