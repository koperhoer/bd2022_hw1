package bd2022.hw1;

// Нам хочется логгировать данные о счётчиках.
import lombok.extern.log4j.Log4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;

import java.time.Instant;

@Log4j
public class MapRedApp {
    public static void main(String[] args) throws Exception {
        // В качестве аргументов нам нужны входная и выходная директории.
        if (args.length != 2) {
            throw new RuntimeException("Arguments: [input folder] [output folder].");
        }
        long start_time, end_time;
        start_time = Instant.now().getEpochSecond();
        // Создадим конфигурацию, дадим джобе имя, назначим все необходимые классы mapred-джобы.
        Configuration conf = new Configuration();
        // Мы ждём, что с выхода mapreduce будут поступать результаты, разделённые запятыми.
        Job job = Job.getInstance(conf, "Clicks heatmap");
        job.setJarByClass(MapRedApp.class);
        job.setMapperClass(HW1Mapper.class);
        job.setReducerClass(HW1Reducer.class);
        // Выходной ключ - логическое имя элемента экрана.
        job.setOutputKeyClass(Text.class);
        // Выходное значение - текстовое обозначение числа нажатий и соответствующей температуры.
        job.setOutputValueClass(IntWritable.class);
        // По варианту, выходной формат - SequenceFile.S
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Назначаем входную и выходную директории.
        Path outputDirectory = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputDirectory);
        // Запускаем джобу. Логгируем наши действия.
        log.info("---------------- JOB START");
        job.waitForCompletion(true);
        end_time = Instant.now().getEpochSecond();
        log.info("---------------- JOB END");
        // Выводим число кликов, не попавших ни в один элемент экрана.
        Counter counter = job.getCounters().findCounter(CounterType.CLICKMISS);
        log.info("---------------- MISSED CLICKS: " + counter.getValue());
        log.info("---------------- TIME ELAPSED (SEC): " + Long.toString(end_time - start_time));
    }
}