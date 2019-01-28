package sub2;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * ��¥ : 2019/01/28
 * ���� : ��ö��
 * ���� : �ϵ� �ʸ��ེ �ܾ� ī��Ʈ �ǽ��ϱ�
 */
public class wordCountMain {

	public static void main(String[] args) {
		
		Configuration conf = new Configuration();
		
		try {
			Job job = new Job(conf, "wordcount");
			
			job.setJarByClass(wordCountMain.class);
			job.setMapperClass(wordCountMapper.class);
			job.setReducerClass(wordCountReducer.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.waitForCompletion(true);
			
		} catch (Exception e) {
			e.printStackTrace();
		} 
		
		System.out.println("�ʸ��ེ ����...");
	}
}