import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.TupleWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class mapper extends Mapper<Text,Text,Text,Text>
{
    @Override
    public void map(Text key,Text value,Context context) throws IOException,InterruptedException
    {
        TupleWritable<Text,Text> NewKey=new TupleWritable<>(key,value);
        IntWriable one= new IntWriable(1);
        context.write(NewKey,one);
    }

}
