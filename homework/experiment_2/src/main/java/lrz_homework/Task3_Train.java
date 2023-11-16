package lrz_homework;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
public class Task3_Train
{
	public static class TrainMapper
		extends Mapper<Object, Text, Text, IntWritable>
	{
		public Task3_Conf task3Conf;
		private final static IntWritable one = new IntWritable(1);
		private Text word;
		@Override
		public void setup(Context context) 
		{
			try{
			task3Conf = new Task3_Conf();
			Configuration conf = context.getConfiguration();
			task3Conf.ReadNaiveBayesConf(conf.get("conf"), conf);
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
				System.exit(1);
			}
			System.out.println("setup");
		}
		@Override
		public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException 
		{
			
			String temp;
			word = new Text();
			String[] vals = value.toString().split(",");
			word.set(vals[vals.length-1]);
			context.write(word, one);
			// Skip the ID chacacteristic of train data
			for(int i = 1; i<vals.length-1; i++)
			{
				word = new Text();
				temp = vals[vals.length-1] + "#" + task3Conf.proNames.get(i-1)+"#"+vals[i];
				word.set(temp);					
				context.write(word, one);
			}

			
		}
	}
	
	public static class TrainReducer
		extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		private IntWritable result = new IntWritable();
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, 
        	Context context) throws IOException, InterruptedException 
		{
			int sum = 0;
			for (IntWritable val : values) 
			{
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
        }
	}
}

