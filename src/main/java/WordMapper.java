import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static java.util.Arrays.stream;

/**
 * Word mapping task for tokenizing the input text based on space and create
 * a list of words, then traverse over all the tokens and emit a key-value
 * pair of each word with a count of one, for example, <Hello, 1>.
 *
 * @author Ephanov Ivan
 */
public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) {

        stream(value.toString().split(" ")).forEach(
                w -> {
                    try {
                        context.write(new Text(w), one);
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });
    }
}
