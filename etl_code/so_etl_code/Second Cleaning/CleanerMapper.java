import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.NullWritable;
public class CleanerMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] cols = line.split(",");
		
		String borough = cols[0];
		String zip = cols[1];
		String critical = cols[2];
		String date = cols[3];
		String score = cols[4];
		String grade = cols[5];
		String latitude = cols[6];
		String longitude = cols[7];
		
		
		if(score.equals("")) { 
			if(grade.equals("N") || grade.equals("Z") || grade.equals("")) {
				context.write(new Text(borough + "," + zip + "," + critical + "," + date  
						+ "," + score + "," + "N" + "," + latitude + ","
						+ longitude + "," + ""), NullWritable.get());
			} else {
				context.write(new Text(borough + "," + zip + "," + critical + "," + date
						+ "," + score + "," + grade + "," + latitude + ","
						+ longitude + "," + ""), NullWritable.get());
			}
		} else if(Integer.parseInt(score) > 17){
			if(grade.equals("N") || grade.equals("Z") || grade.equals("")) {
				context.write(new Text(borough + "," + zip + "," + critical + "," + date
						+ "," + score + "," + "N" + "," + latitude + ","
						+ longitude + "," + "1"), NullWritable.get());
			} else {
				context.write(new Text(borough + "," + zip + "," + critical + "," + date
						+ "," + score + "," + grade + "," + latitude + ","
						+ longitude + "," + "1"), NullWritable.get());
			}
		} else {
			if(grade.equals("N") || grade.equals("Z") || grade.equals("")) {
				context.write(new Text(borough + "," + zip + "," + critical + "," + date
						+ "," + score + "," + "N" + "," + latitude + ","
						+ longitude + "," + "0"), NullWritable.get());
			} else {
				context.write(new Text(borough + "," + zip + "," + critical + "," + date
						+ "," + score + "," + grade + "," + latitude + ","
						+ longitude + "," + "0"), NullWritable.get());
			}
		}
		
		
	}
}
