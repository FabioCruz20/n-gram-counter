package br.com.ggvd.ngramcounter;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.IntWritable;

public class NgramCounterReducer extends
        Reducer<Text, MapWritable, Text, Text> {
    
    // Para escrever as informações associadas a um n-grama
    private Text resultText = new Text(); 
    
    @Override
    public void reduce(Text key, Iterable<MapWritable> values, Context context) 
        throws IOException, InterruptedException {
        
        //* ========= Nome do arquivo atual e parâmetros do programa ========= */
        Configuration config = context.getConfiguration();
        
        int minCount = Integer
            .parseInt(config.get(NgramCounter.MIN_COUNT_KEY));
        
        /* ================== Construção dos n-gramas ======================= */
        Set<String> files = new HashSet<>();
        int total = 0;
        
        for (MapWritable ngram : values) {
            
            IntWritable count = (IntWritable) ngram
                .get(NgramCounterMapper.COUNT_KEY);
            
            Text file = (Text) ngram.get(NgramCounterMapper.FILE_KEY);
            
            // atualiza a contagem de ocorrências e o conjunto de arquivos
            // nos quais o n-grama ocorreu
            total += count.get();
            files.add(file.toString());
        }
        
        // só escreve uma entrada para o n-grama se ele tiver aparecido um
        // número mínimo de vezes
        if (total > minCount) {
            String result = total + " " + String.join(" ", files);
            resultText.set(result);
        
            context.write(key, resultText);
        }
    }
}
