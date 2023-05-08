import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.NullWritable;
public class UniqueGradeCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text critical = new Text();
	private static final IntWritable one = new IntWritable(1);
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] cols = line.split(",");
		
		String criticalCol = cols[5];
	 
		critical.set(criticalCol);
		context.write(critical, one);
	}
}
