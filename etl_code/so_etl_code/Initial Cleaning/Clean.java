import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.NullWritable;
public class Clean {
 public static void main(String[] args) throws Exception {
	 if (args.length != 2) {
		 System.err.println("Usage: Clean Records <input path> <output path>");
		 System.exit(-1);
	 }

	 Job job = new Job();
	 job.setJarByClass(Clean.class);
	 job.setJobName("Clean Records");
	 FileInputFormat.addInputPath(job, new Path(args[0]));
	 FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	 job.setMapperClass(CleanMapper1.class);
	 job.setReducerClass(CleanerReducer.class);
	 job.setOutputKeyClass(Text.class);
	 job.setOutputValueClass(NullWritable.class);
	 job.setNumReduceTasks(1); // 1 Reduce task
	
	 System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
}

