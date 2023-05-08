import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountRecs {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CountRecs <input path> <output path>");
            System.exit(-1);
        }

        //creating the new job and setting the jar and job name
        Job job = new Job();
        job.setJarByClass(CountRecs.class);
        job.setJobName("Lines count");

        //ensuring it reduces down to one file
        job.setNumReduceTasks(1);

        //specifying input and output directory (input output)
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //setting mapper and reducer
        job.setMapperClass(CountRecsMapper.class);
        job.setReducerClass(CountRecsReducer.class);

        //setting output key and value. since its a neighbourhood and val,
        //we make this text and int writable
        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);



        //return exit value upon job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);




    }
}

