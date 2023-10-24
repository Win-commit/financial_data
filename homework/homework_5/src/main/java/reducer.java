import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class reducer extends Reducer<Text,Text,Text,Text>
{
    @Override
    protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException ,InterruptedException
    {
        Set<Text>unique_values=new HashSet<>();
        for(Text value:values){
            unique_values.add(value);
        }

        for(Text UniqueValue:unique_values){
            context.write(key,UniqueValue);
        }

    }

}
    

