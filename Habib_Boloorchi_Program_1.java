import java.util.*; 
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
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
		String MODEL,MAKE;
		MAKE = fields[46];
		MODEL= fields[47];		
		if ( MAKE!= "" &&MODEL!= "" && !MAKE.matches("\\d+")&& !MODEL.matches("\\d+")) {//make sure no corrupt data is there
		context.write(new Text(MAKE), new Text(MODEL));
	}}
	}

//____________________________________________________________________________________________________
class carsReducer 
extends Reducer<Text, Text, Text,IntWritable> {
private HashMap<String,Integer> countMap = new HashMap<String,Integer>();//put data in hash-map for last sorting
public void reduce(Text key, Iterable<Text> values,Context context)
throws IOException, InterruptedException {
	Set<String> SetOfMake = new HashSet<String>();
	for (Text value: values){//this for is to get ride of redundancy
		String stringValue = value.toString().toLowerCase();	
		SetOfMake.add(stringValue);	// put data hash- set remove repeatitive data
}


String[] arrayOfSet = SetOfMake.toArray(new String[SetOfMake.size()]);// convert set to string array
String k = key.toString();

countMap.put(k, SetOfMake.size());

}
protected void cleanup(Context context) throws IOException, InterruptedException {
		ArrayList<Point> cars=new ArrayList<Point>();
		int counter1=0;
		for (Map.Entry<String, Integer> en : countMap.entrySet()){//put data in arraylist oof cars
                Point car= new Point(en.getKey(),en.getValue());
				cars.add(car);
				counter1++;
		}
		
		Collections.sort(cars, new Comparator<Point>() {
    @Override
    public int compare(Point z1, Point z2) {//it is for sorting by values
        if (z1.v1 > z2.v1)
            return -1;
        if (z1.v1 < z2.v1)
            return 1;
        return 0;
    }
});
		int counter = 0;
		for(Point s:cars){
			if (counter ++ == 5) {//to be sure we just have 5 tops
                    break;
                }
                context.write(new Text(s.k1),new IntWritable(s.v1));
		}
		
		}
	
public class Point {

    public final String k1;
    public final  int v1;

    public Point(String k1, int v1) {
        this.k1 = k1;
        this.v1 = v1;
    }
   
}			
}

//////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////
public class Habib_Boloorchi_Program_1 {
public static void main(String[] args) throws Exception {
if (args.length != 2) {
System.err.println("Usage: numberOfMakes <input path> <output path>");
System.exit(-1);
}
Configuration conf =  new Configuration();
Job job = Job.getInstance(conf);

job.setJarByClass(Habib_Boloorchi_Program_1.class);
job.setJobName("Habib_Boloorchi_Program_1");
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
job.setMapperClass(carsMapper.class);
job.setReducerClass(carsReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);//ArrayWritable.class
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(Text.class);

System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}