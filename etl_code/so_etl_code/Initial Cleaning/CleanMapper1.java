import java.util.*;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.NullWritable;
public class CleanMapper1 extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] cols = line.split(",");
        
        String[] boroughs = {"Manhattan", "Brooklyn", "Bronx", "Queens", "Staten Island"};
        List<String> boroughsList = Arrays.asList(boroughs);
        String[] criticals = {"Critical", "Not Critical", "Not Applicable"};
        List<String> criticalsList = Arrays.asList(criticals);
        String grades = "ABCPNZ";
 
        String borough = cols[2];
    	String zip = cols[5];
    	String date = cols[8];
    	String critical = cols[cols.length-14];
    	String score = cols[cols.length-13];
    	String grade = cols[cols.length-12];
    	String latitude = cols[cols.length-8];
    	String longitude = cols[cols.length-7];
    	
    	if((boroughsList.contains(borough) || borough.equals("")) && (zip.matches("\\d+") || zip.equals("")) && (criticalsList.contains(critical) || critical.equals("")) && (score.matches("\\d+") || score.equals("")) && (grades.contains(grade) || grade.equals(""))) {    		
    		context.write(new Text(borough + "," + zip + "," + date + "," + critical + "," + score + "," + grade + "," + latitude + "," + longitude), NullWritable.get());    	
    	}
	}
}

