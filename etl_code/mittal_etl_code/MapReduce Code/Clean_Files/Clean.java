import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Clean {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CountRecs <input path> <output path>");
            System.exit(-1);
        }

        //creating the new job and setting the jar and job name
        Job job = new Job();
        job.setJarByClass(Clean.class);
        job.setJobName("Lines count");

        //ensuring it reduces down to one file
        job.setNumReduceTasks(1);

        //specifying input and output directory (input output)
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //setting mapper and reducer
        job.setMapperClass(CleanMapper.class);
        job.setReducerClass(CleanReducer.class);

        //setting output key and value. since its a neighbourhood and val,
        //we make this text and int writable
        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);



        String fileName = "output/part-r-00000"; // file path
        String content = "Created Date,Incident Zip,Borough,Latitude,Longitude\n";

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
            writer.write(content);
            writer.close();
            System.out.println("File written successfully!");
        } catch (IOException e) {
            System.err.println("Error writing to file: " + e.getMessage());
        }
        //return exit value upon job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);

//        System.out.println("do we even get here");



    }
}

