package lrz_homework;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

public class Task3_TrainResult
{
	public HashMap<String, Integer> freq;
	public Task3_TrainResult()
	{
		freq = new HashMap<String, Integer>();
	}
	public void getData(String file, Configuration conf) throws IOException
	{
		Path data_path = new Path(file);
		Path file_path;
		String temp[], line;
		FileSystem hdfs = data_path.getFileSystem(conf);
		
		FileStatus[] status = hdfs.listStatus(data_path);
		
		for(int i = 0; i<status.length; i++)
		{
			file_path = status[i].getPath();			
			if(hdfs.getFileStatus(file_path).isDirectory() == true)
				continue;

			line = file_path.toString();
			temp = line.split("/");
            // temp[temp.length-1] is the name of a file,in this example ,we want to find the outcome of train process,which is named by the beginning of 'part'
			if(temp[temp.length-1].substring(0,5).equals("part-") == false)
				continue;
			System.out.println(line);
			FSDataInputStream fin = hdfs.open(file_path);
			InputStreamReader inr = new InputStreamReader(fin);
			BufferedReader bfr = new BufferedReader(inr);
			while((line = bfr.readLine()) != null)
			{	
				String res[] = line.split("\t");
				freq.put(res[0], new Integer(res[1]));
				System.out.println(line);
			}
			bfr.close();
			inr.close();
			fin.close();
		}
	}
	
}
