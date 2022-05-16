import java.io.IOException;
import java.util.ArrayList;
import java.io.FileReader;
import java.io.BufferedReader;
import java.lang.Math;
import java.io.*;
import java.util.*;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

public class AttributeEntropy 

	{

  		public static class AttributeMapper
       		extends Mapper<Object, Text, Text, Text>
			{
				private final static Text attribute = new Text();
    				private final static Text attr_class = new Text();
    				private ArrayList<String> attributes = new ArrayList<String>();
				@Override
    				protected void setup(Context context) throws IOException, InterruptedException 
					{

						attributes.add("outlook");
						attributes.add("temperature");
						attributes.add("construction");
	
    					}

    				public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
					{
			
     	 					String[] tokens  = value.toString().split(",");
      	 					if (tokens.length == 4) 
						{
							for (int i=0; i<tokens.length - 1; i++) 
							{
								attribute.set(attributes.get(i));
								attr_class.set(tokens[i] + "," + tokens[tokens.length - 1]);
				
								context.write(attribute, attr_class);
							}
      						}

   	 				}
   	 
  			}

		public static class AttributeReducer extends Reducer<Text,Text,Text,DoubleWritable> 
			{

    				private static final DoubleWritable result = new DoubleWritable();

    				public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
					{

      						ArrayList<String> s1 = new ArrayList<String>();
      						ArrayList<String> s2 = new ArrayList<String>();

      						for (Text pair : values) 
							{
      								String[] tokens  = pair.toString().split(",");
								s1.add(tokens[0]);
								s2.add(tokens[1]);
      							}
      						result.set(entropy(s1,s2));
      						context.write(key, result);
    					}

				public HashMap<String, Integer> mapToID(ArrayList<String> arr) 
					{
     						HashMap<String, Integer> hmap = new HashMap<>();
  						int id = 0;

						for (int i=0; i<arr.size(); i++) 
							{
	    							Integer count = hmap.get(arr.get(i));
	    							if (count == null) 
	    								{
										hmap.put(arr.get(i), id++);
	    								}
							}
						return hmap;
   					}

    				public double entropy(ArrayList<String>attrib, ArrayList<String>classes) 
    					{
    						System.out.println(attrib);
    						System.out.println(classes);
						HashMap<String, Integer> attribID = mapToID(attrib);
						HashMap<String, Integer> classLabel = mapToID(classes);

   						int numValues = attribID.size();
   						int numClasses = classLabel.size();
   						int matrix[][] = new int[numValues][numClasses];
   						int aID, cID;

						// Initialize matrix to have zero values
   						for (int i=0; i<numValues; i++) 
   							{
       	    						for (int j=0; j<numClasses; j++) 
       	    							{
	   									matrix[i][j] = 0;
       	    							}
   							}

						// Count frequency for each (attribute value, class) combination

   						int rowsum[] = new int[numValues];
   						int total = 0;
   						for (int i=0; i<attrib.size(); i++) 
   							{
       	    						aID = attribID.get(attrib.get(i)).intValue();
       	    						cID = classLabel.get(classes.get(i)).intValue();
       	    						matrix[aID][cID] += 1;
       	    						rowsum[aID] += 1;
       	    						total += 1;
   							}

						// Calculate entropy for each attribute value and
						// weighted average for the entire attribute
   						double entValues[] = new double[numValues];
   						double p, e, eTotal = 0.;

   						for (int i=0; i<numValues; i++) 
   							{
       	    						entValues[i] = 0;
       	    						for (int j=0; j<numClasses; j++) 
       	    							{
	   									p = matrix[i][j]*1.0/(rowsum[i] + 1.0e-14);
	   									e = (-1.0*p*Math.log(p + 1.0e-14)/Math.log(2.0));
	   									entValues[i] += e;
       	    							}
       	    						eTotal += (rowsum[i]*entValues[i]/total);
   							}

   						return eTotal;
    					}

 			}
 			
 			public static void main(String[] args) throws Exception 
					{

    						Configuration conf = new Configuration();
    						Job job = Job.getInstance(conf,"Congestion Attribute Entropy");
    						job.setJarByClass(AttributeEntropy.class);
    						job.setMapperClass(AttributeMapper.class);
    						job.setReducerClass(AttributeReducer.class);
    						job.setMapOutputKeyClass(Text.class);
    						job.setMapOutputValueClass(Text.class);
    						job.setOutputKeyClass(Text.class);
    						job.setOutputValueClass(DoubleWritable.class);
    						FileInputFormat.addInputPath(job, new Path(args[0]));
    						FileOutputFormat.setOutputPath(job, new Path(args[1]));
    						System.exit(job.waitForCompletion(true) ? 0 : 1);
  					}
 	} 	

