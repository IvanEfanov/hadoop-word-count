import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;

/**
 * This function is called after the map method and receives keys which
 * in this case are the word and also the corresponding values. Reduce method iterates over the values,
 * adds them and reduces to a single value before finally writing the word and the number of occurrences
 * of the word to the output file.
 *
 * @author Ephanov Ivan
 */
public class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = StreamSupport.stream(values.spliterator(), false)
                .map(IntWritable::get)
                .reduce(0, Integer::sum);

        context.write(key, new IntWritable(sum));
    }
}
