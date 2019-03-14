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
		String line = value.toString().toLowerCase();//for making sure all line is string and lower case
		String[] fields = line.split(",");// split each row in an array of strings
		String FUEL,phev;
			phev = fields[49];//choose column phevBlended as key
			FUEL= fields[0];		// choose column barrels as value
		if (  phev!= "" &&FUEL!= "" && !phev.matches("\\d+")&& FUEL.matches("\\d*\\.?\\d*")) {
		context.write(new Text(phev), new Text(FUEL));
	}}
	}
//}



//____________________________________________________________________________________________________
class carsReducer 
extends Reducer<Text, Text, Text,FloatWritable> {
	private float electricCarsAve,gasolinCarsAve;// global variable for get data from reduce and fetch in cleanup
	public void reduce(Text key, Iterable<Text> values,Context context)
	throws IOException, InterruptedException {

		int counter = 0;
		float sum =0;
		for (Text value: values){//sum up whole values
			String FloatValue = value.toString();	
			counter++;
			sum+= Float.parseFloat(FloatValue); 
		}


		String k = key.toString().toLowerCase();
		float average= sum/counter;

		if(k.contains("false")){//see if it is not using gasoline
			electricCarsAve= average;	
		}
		if(k.contains("true")){//using gasoline	
			gasolinCarsAve=average;	
		}


}


protected void cleanup(Context context) throws IOException, InterruptedException {
		
                context.write(new Text("Difference= "),new FloatWritable(electricCarsAve-gasolinCarsAve));//output difference
		}
		

}	
	



/////////////////////////////////////////////////////////////////////////////////



////////////////////////////////////////////////////////////////////////////////////////
public class Habib_Boloorchi_Program_4 {
public static void main(String[] args) throws Exception {
if (args.length != 2) {
System.err.println("Usage: numberOfMakes <input path> <output path>");
System.exit(-1);
}
Configuration conf =  new Configuration();
Job job = Job.getInstance(conf);

job.setJarByClass(Habib_Boloorchi_Program_4.class);
job.setJobName("Habib_Boloorchi_Program_4");
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