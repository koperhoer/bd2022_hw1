import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import bd2022.hw1.HW1Mapper;
import bd2022.hw1.HW1Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

// По два теста на маппер, на редьюсер и на их связку.
public class HW1MapperReducerTest {
    // Апачевские драйверы для наших классов.
    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, Text> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, Text> mapReduceDriver;

    // Инициализация соответствующих драйверов.
    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    // Тест маппера №1: клик на рекламу. На выходе - название элемента, единица.
    @Test
    public void testMapper1() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text("321,932,1464575,08:28:00"))
                .withOutput(new Text("tabloid_rubbish"), new IntWritable(1))
                .runTest();
    }

    // Тест маппера №2: клик на поле поиска. На выходе - название элемента, единица.
    @Test
    public void testMapper2() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text("554,843,1464575,10:16:04"))
                .withOutput(new Text("search_with_google"), new IntWritable(1))
                .runTest();
    }

    // Тест редьюсера №1: три клика на кнопку "Домой". На выходе - название элемента, три клика, низкая "температура" кнопки.
    @Test
    public void testReducer1() throws IOException {
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver
                .withInput(new Text("main_page"), values)
                .withOutput(new Text("main_page"), new Text("clicks=3,heat=low"))
                .runTest();
    }

    // Тест редьюсера №2: миллион клика на рекламу. На выходе - название элемента, миллион кликов, высокая "температура" кнопки.
    @Test
    public void testReducer2() throws IOException {
        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(1000000));
        reduceDriver
                .withInput(new Text("ad_banner"), values)
                .withOutput(new Text("ad_banner"), new Text("clicks=1000000,heat=high"))
                .runTest();
    }

    // Тест связки №1: два клика на кнопку "Список тем". На выходе - название элемента, два клика, низкая "температура" кнопки.
    @Test
    public void testMapReduce1() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(), new Text("438,619,1464575,00:18:50"))
                .withInput(new LongWritable(), new Text("452,749,1464575,19:34:04"))
                .withOutput(new Text("all_topics"), new Text("clicks=2,heat=low"))
                .runTest();
    }

    // Тест связки №2: два клика на кнопку "Все посты". На выходе - название элемента, два клика, низкая "температура" кнопки.
    @Test
    public void testMapReduce2() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(), new Text("738,269,1464575,15:42:17"))
                .withInput(new LongWritable(), new Text("779,339,1464575,17:42:46"))
                .withOutput(new Text("all_posts"), new Text("clicks=2,heat=low"))
                .runTest();
    }
}