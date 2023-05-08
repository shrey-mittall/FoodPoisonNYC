import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountRecsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //simple way to ensure that we are not reading the header
        if(value.toString().charAt(0) != 'U') {
            context.write(new Text("Lines in original dataset, before cleaning"), new IntWritable(1));
        }

    }

}

