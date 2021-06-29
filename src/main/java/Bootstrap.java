import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


/**
 * Following code checks for the correct input arguments which are the paths of the input and output files.
 * Followed by setting up and running the job. At the end, it informs the user if the job is completed successfully or not.
 * The resultant file with the word counts and the corresponding number of occurrences will be present in the provided output path.
 *
 * @author Ephanov Ivan
 */
public class Bootstrap extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        if (ToolRunner.run(new Bootstrap(), args) == 1) {
            System.out.println("Job was successful");
        } else {
            System.out.println("Job was not successful");
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s needs two arguments, input and output files \n", getClass().getSimpleName());
            return -1;
        }

        return getJob(args).waitForCompletion(true) ? 0 : 1;
    }


    private Job getJob(String[] args) throws IOException {
        Job job = new Job();
        job.setJarByClass(Bootstrap.class);
        job.setJobName("WordCounter");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);
        return job;
    }
}
