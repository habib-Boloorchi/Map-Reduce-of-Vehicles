import java.util.*; 
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
////////////////////////////////////////////////////////////////////////////////
class carsMapper
	extends Mapper<LongWritable, Text, Text, Text> {
	private static final int makeOffset = 0;
	private static final int modelOffset = 0;
	
	
	public void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException {
		String line = value.toString().toLowerCase();
		String[] fields = line.split(",");// put each row in an array of strings
		String fueltype1,city08;
		fueltype1 = fields[30];//choose fueltype1 as key
		city08= fields[4];//choose city08 as value
		if (  fueltype1!= "" &&city08!= "" && !fueltype1.matches("\\d+")&& city08.matches("\\d*\\.?\\d*")) {//check if thereis right data in key and value
		context.write(new Text(fueltype1), new Text(city08));
	}}
	}
//}



//____________________________________________________________________________________________________
class carsReducer 
extends Reducer<Text, Text, Text,FloatWritable> {
public void reduce(Text key, Iterable<Text> values,Context context)
throws IOException, InterruptedException {
	int counter = 0;
	float sum =0;
	for (Text value: values){
		String FloatValue = value.toString();	
		counter++;
		sum+= Float.parseFloat(FloatValue); 
		
}

String k = key.toString().toLowerCase();
float average= sum/counter;
if (k.contains("electricity")&& !(k.contains("regular"))&& !(k.contains("premium")))//check if it is jst electrical
context.write(key	,new FloatWritable(sum/counter));
}	
}


/////////////////////////////////////////////////////////////////////////////////



////////////////////////////////////////////////////////////////////////////////////////
public class Habib_Boloorchi_Program_3 {
public static void main(String[] args) throws Exception {
if (args.length != 2) {
System.err.println("Usage: numberOfMakes <input path> <output path>");
System.exit(-1);
}
Configuration conf =  new Configuration();
Job job = Job.getInstance(conf);

job.setJarByClass(Habib_Boloorchi_Program_3.class);
job.setJobName("Habib_Boloorchi_Program_3");
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
job.setMapperClass(carsMapper.class);
job.setReducerClass(carsReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(FloatWritable.class);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(Text.class);

System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}