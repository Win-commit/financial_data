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
        // f**k ,Text.class  don't override hashcode() way and equal() way,So Set can't compare two Text objects
        Set<String> unique_values=new HashSet<>();
        for(Text value:values){
            unique_values.add(value.toString());
        }

        for(String UniqueValue:unique_values){
            context.write(key,new Text(UniqueValue));
        }

    }

}
    

