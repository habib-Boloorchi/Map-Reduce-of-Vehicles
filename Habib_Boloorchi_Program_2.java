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
//____________________________________________________________________________________________________	
	public void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException {
		String line = value.toString().toLowerCase();
		String[] fields = line.split(",");// put each row in an array of strings
		String FUEL,MAKE;
		
			MAKE = fields[46];
			FUEL= fields[0];
		
		if ( MAKE!= "" &&FUEL!= "" && !MAKE.matches("\\d+")&& FUEL.matches("\\d*\\.?\\d*")) {
		context.write(new Text(MAKE), new Text(FUEL));
	}}
	}


//____________________________________________________________________________________________________
class carsReducer 
extends Reducer<Text, Text, Text,FloatWritable> {
private Map<String,Float> countMap = new HashMap<String,Float>();//Hash map for put make and average in it

public void reduce(Text key, Iterable<Text> values,Context context)
throws IOException, InterruptedException {
	int counter = 0;
	float sum =0;
	for (Text value: values){
		String FloatValue = value.toString();	
		counter++;
		sum+= Float.parseFloat(FloatValue); 
		
}

String k = key.toString();
float average= sum/counter;
countMap.put(k, average);//put data in hash-map 

}
protected void cleanup(Context context) throws IOException, InterruptedException {// we need a function that run once in the end
		ArrayList<Point> cars=new ArrayList<Point>();
		//each point is a car;
		int counter1=0;
		for (Map.Entry<String, Float> en : countMap.entrySet()){
                Point car= new Point(en.getKey(),en.getValue());
				cars.add(car);
				counter1++;
		}
		
		Collections.sort(cars, new Comparator<Point>() {
    @Override
    public int compare(Point z1, Point z2) {//over ride to sort by value of each car
        if (z1.v1 > z2.v1)
            return -1;
        if (z1.v1 < z2.v1)
            return 1;
        return 0;
    }
});
		int counter = 0;
		for(Point s:cars){
			
                context.write(new Text(s.k1),new FloatWritable(s.v1));
		}
		
		}
	
public class Point {//class contain two varable

    public final String k1;
    public final  float v1;

    public Point(String k1, float v1) {
        this.k1 = k1;
        this.v1 = v1;
    }    
}		
}


/////////////////////////////////////////////////////////////////////////////////



////////////////////////////////////////////////////////////////////////////////////////
public class Habib_Boloorchi_Program_2 {
public static void main(String[] args) throws Exception {
if (args.length != 2) {
System.err.println("Usage: numberOfMakes <input path> <output path>");
System.exit(-1);
}
Configuration conf =  new Configuration();
Job job = Job.getInstance(conf);

job.setJarByClass(Habib_Boloorchi_Program_2.class);
job.setJobName("Habib_Boloorchi_Program_2");
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