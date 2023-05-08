import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.NullWritable;
public class UniqueBorough {
 public static void main(String[] args) throws Exception {
	 if (args.length != 2) {
		 System.err.println("Usage: Unique Borough Count <input path> <output path>");
		 System.exit(-1);
	 }

	 Job job = new Job();
	 job.setJarByClass(UniqueBorough.class);
	 job.setJobName("Unique Borough Count");
	 FileInputFormat.addInputPath(job, new Path(args[0]));
	 FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	 job.setMapperClass(UniqueBoroughMapper.class);
	 job.setReducerClass(UniqueBoroughReducer.class);
	 job.setOutputKeyClass(Text.class);
	 job.setOutputValueClass(IntWritable.class);
	 job.setNumReduceTasks(1); // 1 Reduce task
	
	 System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
}

