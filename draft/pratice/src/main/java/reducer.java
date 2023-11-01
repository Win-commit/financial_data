import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class reducer  extends Reducer<Text,Text,Text,Text>
{
    @Override
    protected void reduce(TupleWriable<Text,Text> key,Iterable<Intwriable> values,Context context) throws IOException ,InterruptedException{
        context.write(key[0],new Intwriable(1));
    }
}
