package lrz_homework;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Task2
{
public static class mapper extends Mapper<LongWritable,Text,Text,LongWritable>
{   @Override
    public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
    {
        String[] val=value.toString().split(",");

        Text NewKey=new Text(val[25]);
        context.write(NewKey,new LongWritable(1));
    }
}

public static class reducer extends Reducer<Text,LongWritable,Text,LongWritable>
{   TreeMap<Long,String> sortWeekday= new TreeMap<>(Collections.reverseOrder());
    @Override
    public void reduce(Text key,Iterable<LongWritable> val,Context context)throws IOException ,InterruptedException
    {
        Iterator<LongWritable> iter=val.iterator();
        Long sum=0L;
        while(iter.hasNext()){
            sum+=iter.next().get();
        }
        // context.write(key,new LongWritable(sum));
        sortWeekday.put(sum,key.toString());
    }

    public void cleanup(Context context)throws IOException ,InterruptedException
    {
        for(Long count:sortWeekday.keySet())
        {
            context.write(new Text(sortWeekday.get(count)),new LongWritable(count));
        }
    }

}


    public static void main(String[] args) 
    {
        try{
            Configuration conf=new Configuration();
            Job job = Job.getInstance(conf, "experiment_2_Task2");
            job.setJarByClass(Task2.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(mapper.class);
            job.setReducerClass(reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            FileInputFormat.addInputPath(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job,new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        }
        catch(Exception e)
        {e.printStackTrace();}
    }

}

