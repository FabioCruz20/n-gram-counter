package br.com.ggvd.ngramcounter;

import java.io.IOException;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class NgramCounter {
    
    // usado como chaves para passar ao mapper e ao reducer os argumentos do 
    // grau do n-grama e o número mínimo de ocorrências do n-grama
    public static final String NGRAM_DEGREE_KEY = "NGRAM_DEGREE";
    public static final String MIN_COUNT_KEY = "MIN_COUNT";
 
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
         
        // Captura o parâmetros passados após o nome da Classe driver.
        String ngramDegree = args[0];
        String minCount = args[1];
        Path inputPath = new Path(args[2]);
        Path outputDir = new Path(args[3]);
 
        // Criar uma configuração
        Configuration conf = new Configuration(true);
        conf.set(NgramCounter.NGRAM_DEGREE_KEY, ngramDegree);
        conf.set(NgramCounter.MIN_COUNT_KEY, minCount);
 
        // Criar o job
        Job job = new Job(conf, "NgramCounter");
        job.setJarByClass(NgramCounter.class);
 
        // Definir classes para Map e Reduce
        job.setMapperClass(NgramCounterMapper.class);
        job.setReducerClass(NgramCounterReducer.class);
        job.setNumReduceTasks(1);
 
        // Definir as chaves e valor
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
 
        // Entradas
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);
 
        // Saidas
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        // EXcluir saida se existir
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);
 
        // Executa job
        int code = job.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
 
    }
}
