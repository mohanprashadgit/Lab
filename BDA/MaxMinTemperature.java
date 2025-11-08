import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxMinTemperature {

    // Mapper Class
    public static class TempMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text type = new Text("temp"); // single key to send all data to one reducer

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.startsWith("year") || line.isEmpty()) return; // skip header
            String[] parts = line.split(",");
            int temp = Integer.parseInt(parts[1].trim());
            context.write(type, new IntWritable(temp));
        }
    }

    // Reducer Class
    public static class TempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int maxTemp = Integer.MIN_VALUE;
            int minTemp = Integer.MAX_VALUE;

            // find max and min temperatures
            for (IntWritable val : values) {
                int t = val.get();
                if (t > maxTemp) maxTemp = t;
                if (t < minTemp) minTemp = t;
            }

            context.write(new Text("Maximum Temperature:"), new IntWritable(maxTemp));
            context.write(new Text("Minimum Temperature:"), n
