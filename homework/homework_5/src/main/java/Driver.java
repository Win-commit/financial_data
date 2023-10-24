import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class Driver {
    public static void main(String[] args){
        try{
            Configuration conf=new Configuration();
            Job job = Job.getInstance(conf, "homework_5");
            job.setJarByClass(Driver.class);
            job.setInputFormatClass(ExcelInputFormat.class);
            job.setMapperClass(mapper.class);
            job.setReducerClass(reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job,new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        catch(Exception e)
        {e.printStackTrace();}
    }
}
