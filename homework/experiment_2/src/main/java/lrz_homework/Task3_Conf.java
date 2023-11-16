package lrz_homework;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Task3_Conf
{
	public int dimen;
	public int class_num;
	public ArrayList<String> classNames;
	public ArrayList<String> proNames;
	public ArrayList<Integer>	proRanges;
	
	public Task3_Conf()
	{
		dimen = class_num = 0;
		classNames = new ArrayList<String>();
		proNames = new ArrayList<String>();
	}
	
	public void ReadNaiveBayesConf(String file, Configuration conf) throws Exception
	{	
		Path conf_path = new Path(file);
		FileSystem hdfs = conf_path.getFileSystem(conf);
		FSDataInputStream fsdt = hdfs.open(conf_path);
		Scanner scan = new Scanner(fsdt);
	
		// 读取第一行：类别数量和类别名称
		String str = scan.nextLine();
		String[] vals = str.split(",");
		class_num = Integer.parseInt(vals[0]);
		for (int i = 1; i < vals.length; i++) {
			classNames.add(vals[i]);
		}
	
		// 读取第二行：特征数量和特征名称
		if (scan.hasNextLine()) {
			str = scan.nextLine();
			vals = str.split(",");
			dimen = Integer.parseInt(vals[0]);
			for (int i = 1; i < vals.length; i++) {
				proNames.add(vals[i]);
			}
		}
	
		fsdt.close();
		scan.close();
	}
}
