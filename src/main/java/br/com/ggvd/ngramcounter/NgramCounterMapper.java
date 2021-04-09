package br.com.ggvd.ngramcounter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.MapWritable;


public class NgramCounterMapper extends Mapper<Object, Text, Text, MapWritable> {
    
    // Para escrever o ngrama como chave de um par
    private Text ngramText = new Text();
    
    // Para definir as chaves de um MapWritable associado a um n-grama
    public static final Text COUNT_KEY = new Text("COUNT");
    public static final Text FILE_KEY  = new Text("FILE");
    
    // Para definir os valores de um MapWritable associado a um n-grama
    private static final IntWritable COUNT_VALUE = new IntWritable(1);
    private Text fileValue = new Text();

    @Override
    public void map(Object key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        /* ========= Nome do arquivo atual e parâmetros do programa ========= */
        
        // nome do arquivo que o processo está manipulando
        String filename = ((FileSplit) context.getInputSplit())
            .getPath().getName();
        
        fileValue.set(filename);
        
        Configuration config = context.getConfiguration();
        
        // Grau do n-grama passado como argumento para o programa
        int ngramDegree = Integer
            .parseInt(config.get(NgramCounter.NGRAM_DEGREE_KEY));
        
        /* ================== Construção dos n-gramas ======================= */
        String[] tokens = value.toString().split(" ");
        
        int endIndex = tokens.length - ngramDegree;
        for (int i = 0 ; i <= endIndex; i++) {
            
            MapWritable ngramInfo = new MapWritable();
            
            // construção do n-grama começando da i-ésima palavra
            String ngramContent = "";
            for (int j = i; j < i + ngramDegree; j++) {
                
                ngramContent += tokens[j] + " ";
            }
            
            ngramText.set(ngramContent);

            // número atual de ocorrências da palavra e arquivo onde ocorreu.
            ngramInfo.put(COUNT_KEY, COUNT_VALUE);
            ngramInfo.put(FILE_KEY, fileValue);
            
            context.write(ngramText, ngramInfo);
        }
    }
}
