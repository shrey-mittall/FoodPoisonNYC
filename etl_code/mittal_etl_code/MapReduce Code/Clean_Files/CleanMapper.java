import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {



//        List<Integer> columnsToRemove = Arrays.asList(0,3,4,5,6,7,9,10,11,12,13,14,15,16,17,18,19,20,
//                21,22,24,25,26,27,28,29,30,31,32,33,34,37);
        List<Integer> indicesToKeep = Arrays.asList(1, 8, 23, 35, 36);
        String[] currRow = value.toString().split(",");


        //simple way to ensure that we are not reading the header
        //after careful analysis, rows that do not have 39 values have 28
        //and are the ones which do not have an address. since this is
        //central to the analysis, we cap it at only rows with 39 vals
        if(value.toString().charAt(0) != 'U' && currRow.length == 39) {
            System.out.println(Arrays.toString(currRow));

            List<String> cleanedColumns = indicesToKeep.stream().map(index -> currRow[index]).collect(Collectors.toList());
            String finalRow = String.join(",", cleanedColumns);
            System.out.println(finalRow.split(",").length-1);
            context.write(new Text(finalRow), new IntWritable(1));
        }

    }

}


//code to identify which rows have wonky count of values
//        if(currRow.length != 39) {
////            System.out.println(value.toString().split(",")[0]);
//            System.out.println(currRow.length);
//        }

