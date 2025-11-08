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
            context.write(new Text("Minimum Temperature:"), new IntWritable(minTemp));
        }
    }

    // Main Method
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxMinTemperature <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Max and Min Temperature");
        job.setJarByClass(MaxMinTemperature.class);
        job.setMapperClass(TempMapper.class);
        job.setReducerClass(TempReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
