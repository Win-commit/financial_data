package lrz_homework;
import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

public class Task3_Test
{
	public static class TestMapper 
		extends Mapper<Object, Text, Text, Text>
	{
		public Task3_Conf task3Conf;
		public Task3_TrainResult task3TData;

        @Override
		public void setup(Context context)
		{			
			try{
			Configuration conf = context.getConfiguration();
			
			task3Conf = new Task3_Conf();
			task3Conf.ReadNaiveBayesConf(conf.get("conf"), conf);
			task3TData = new Task3_TrainResult();
			task3TData.getData(conf.get("train_result"), conf);
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
				System.exit(1);
			}
		}

		@Override
		public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException 
		{
			
			String  temp;
			BigInteger  fXyi=BigInteger.ONE,fyi=BigInteger.ZERO,fxjyi=BigInteger.ZERO;
			Text cls;
			
			String[] vals = value.toString().split(",");
            String y_true=vals[vals.length-1];
			BigInteger maxf = new BigInteger("-100");
			int idx = -1;
			Text id = new Text(vals[0]);
			for(int i = 0; i<task3Conf.class_num; i++)
			{
				fXyi = BigInteger.ONE;
				String classname = task3Conf.classNames.get(i);
				Integer integer = task3TData.freq.get(classname);
				if(integer == null)
					fyi = BigInteger.ZERO;
				else
					fyi = BigInteger.valueOf(integer.intValue());

				for(int j = 1; j<vals.length-1; j++)
				{
					temp = classname + "#" + task3Conf.proNames.get(j-1) + "#" + vals[j];
						
					integer = task3TData.freq.get(temp);
					if(integer == null)
						fxjyi = BigInteger.ZERO;
					else
						fxjyi = BigInteger.valueOf(integer.intValue());

					fXyi = fXyi.multiply(fxjyi);
				}
				BigInteger product = fyi.multiply(fXyi);
				if(product.compareTo(maxf) > 0)
				{
					maxf = product;
					idx = i;
				}
			}
		
			cls = new Text(task3Conf.classNames.get(idx));
			context.write(id, cls);

            String res="1";
            if(y_true.equals(cls.toString()))
            {  
                res+="#"+"1";
            }
            else
            {
               res+="#"+"0";
            }
            context.write(new Text("accuracy"), new Text(res));

		}
	}


    public static class TestReducer 
		extends Reducer<Text, Text, Text, Text>
        {
            @Override
            public void reduce(Text key,Iterable<Text> value,Context context)
                                        throws IOException, InterruptedException
            {
                if (key.toString().equals("accuracy"))
                {
                    float total=0f;
                    float T=0f;
                    for(Text val:value){
                        String[] nums=val.toString().split("#");
						total+=Float.parseFloat(nums[0]);
						T+=Float.parseFloat(nums[1]);
					}
                    float accuracy= T*100/total;
                    String accuracyString=String.format("%.2f",accuracy);
                    context.write(key,new Text(accuracyString));
                }
                else
                {
                    for(Text val:value){context.write(key,val);}
                }

            }

        }


	public static class TestCombiner 
		extends Reducer<Text, Text, Text, Text>
		{
			@Override
			public void reduce(Text key,Iterable<Text> value,Context context)
													throws IOException, InterruptedException
			{
				if (key.toString().equals("accuracy"))
                {
                    float total=0f;
                    float T=0f;
                    for(Text val:value)
					{
                        String[] nums=val.toString().split(",");
						total+=Float.parseFloat(nums[0]);
						T+=Float.parseFloat(nums[1]);
					}
					String totalString=String.format("%.2f",total);
					String TString=String.format("%.2f",T);
					context.write(key, new Text(totalString+","+TString));

			    }
				else
                {
                    for(Text val:value){context.write(key,val);}
                }
		    }
		}
}
