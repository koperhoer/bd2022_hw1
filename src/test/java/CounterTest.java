import bd2022.hw1.CounterType;
import bd2022.hw1.HW1Mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

// Тесты на нулевое, единичное и множественное значения счётчика кликов, не попавших по элементам.
public class CounterTest {
    // Апачевский драйвер для нашего маппера и его инициализация.
    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    // Тест на единичное значение. На входе - клик в никуда. На выходе - единица.
    @Test
    public void testMapperCounterOne() throws IOException  {
        mapDriver
                .withInput(new LongWritable(), new Text("0,0,1464575,08:28:00"))
                .runTest();
        assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()
                .findCounter(CounterType.CLICKMISS).getValue());
    }

    // Тест на нулевое значение. На входе - клик в рекламу. На выходе - ноль.
    @Test
    public void testMapperCounterZero() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text("321,932,1464575,08:28:00"))
                .withOutput(new Text("tabloid_rubbish"), new IntWritable(1))
                .runTest();
        assertEquals("Expected 1 counter increment", 0, mapDriver.getCounters()
                .findCounter(CounterType.CLICKMISS).getValue());
    }

    // Тест на множественное значение. На входе - клик в рекламу и два клика в никуда. На выходе - двойка.
    @Test
    public void testMapperCounters() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text("321,932,1464575,08:28:00\n"))
                .withInput(new LongWritable(), new Text("0,0,1464575,08:28:00\n"))
                .withInput(new LongWritable(), new Text("1920,1080,1464575,08:28:00\n"))
                .withOutput(new Text("tabloid_rubbish"), new IntWritable(1))
                .runTest();
        assertEquals("Expected 2 counter increment", 2, mapDriver.getCounters()
                .findCounter(CounterType.CLICKMISS).getValue());
    }
}
