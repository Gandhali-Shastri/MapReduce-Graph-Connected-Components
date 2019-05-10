import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {


public int tag;                  // 0 for a graph vertex, 1 for a group number
public long group;                // the group where this vertex belongs to
public long VID; 					// the vertex ID
public long size;					
public Vector<Long> adjacent = new Vector<Long>();	// the vertex neighbors

Vertex(){}

Vertex(int tag,long group,long VID,Vector<Long> adjacent)
  {
	this.tag=tag;
	this.group=group;
	this.VID=VID;
	this.adjacent=adjacent;
	size=adjacent.size();
  }
  
  Vertex(int tag,long group)
  {
	  this.tag=tag;
	  this.group=group;
  }
  
 
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		VID =in.readLong();
		group =in.readLong();
		adjacent=new Vector<Long>();
		tag=in.readShort();
		 size=in.readLong();
		 
        for (int i=0;i<size;i++)
        {
          adjacent.add(in.readLong());
//          System.out.println("read adj: "+ adjacent.get(i));
        }
	}
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(VID);
		out.writeLong(group);
		out.writeShort(tag);
		out.writeLong(size);
		for (int i=0;i<adjacent.size();i++)
		{
			out.writeLong(adjacent.get(i));
		}

	}
}

public class Graph {

	public static class Mapper1 extends Mapper<Object,Text,LongWritable,Vertex> {   
		@Override
		public void map(Object key, Text value,
				Context context ) throws IOException, InterruptedException
		{  

			String line = value.toString();
			String[] vertices =  line.split(",");


			long vertexId = Long.parseLong(vertices[0]);
			
//			System.out.println("ver id" + vertexId);

			Vector<Long> adjacent = new Vector<Long>();
//			System.out.println("Mapper 1");
			for (int i = 1; i < vertices.length; i++) {
				Long adj=Long.parseLong(vertices[i]);

				adjacent.add(adj);
//				System.out.println("I adj"+ i + adj);
			}

			context.write(new LongWritable(vertexId),new Vertex((short)0,vertexId,vertexId,adjacent));

		}
	}
	
	public static class Reducer1 extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {
		@Override
		public void reduce(LongWritable key, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {
//			System.out.println("Reducer 1");
			for(Vertex v:values) {
				context.write(key, new Vertex(v.tag,v.group,v.VID,v.adjacent));
				
//				System.out.println("key "+ key + "Vertex: " + v.tag + v.group + v.VID + v.adjacent);
			}

		}
	}

	public static class Mapper2 extends Mapper<LongWritable,Vertex,LongWritable,Vertex> {   
		@Override
		public void map(LongWritable key, Vertex vertex,
				Context context ) throws IOException, InterruptedException
		{    		      
//			System.out.println("Mapper 2");
			context.write(new LongWritable(vertex.VID), vertex);

				for(Long n : vertex.adjacent) {
					context.write(new LongWritable(n), new Vertex((short)1,vertex.group));
//					System.out.println("vertex grp: "+ vertex.group);
					
				}
//				System.out.println("M2"+ vertex.adjacent);

		}
	}

	public static class Reducer2 extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {
		@Override
		public void reduce(LongWritable vertexId, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {
			
			Object adj = null;	
			long m = Long.MAX_VALUE;
			
//			System.out.println("Reducer 2");
			
			for(Vertex v:values) {
				
				if((v.tag==0))
				{
					adj =v.adjacent.clone();
//					System.out.println(adj);
				}

				m = Long.min(m,v.group);
//				System.out.println("min " + m);
				
			}
			@SuppressWarnings("unchecked")
			Vector<Long> adjcnt = (Vector<Long>) adj;
//			System.out.println(adjcnt);
			context.write(new LongWritable(m), new Vertex((short)0,m,vertexId.get(),adjcnt));
		}
	}
	
	public static class Mapper3 extends Mapper<LongWritable,Vertex,LongWritable,IntWritable > 
	{
        @Override
        public void map ( LongWritable key, Vertex value, Context context )
                        throws IOException, InterruptedException {
            context.write(new LongWritable(value.group),new IntWritable(1));
            
        }
    }		

 public static class Reducer3 extends Reducer<LongWritable,IntWritable,LongWritable,LongWritable> 
	{        
        @Override
        public void reduce ( LongWritable key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
        	long m=0L;
        	for(IntWritable v:values)
        	{
        		m = m + Long.valueOf(v.get());
        	}
        	context.write(key,new LongWritable(m));
        }
	}
 




	public static void main ( String[] args ) throws Exception {
		Job job1 = Job.getInstance();
		job1.setJobName("Job1");
		/* ... First Map-Reduce job to read the graph */

		job1.setJarByClass(Graph.class);
		job1.setOutputKeyClass(LongWritable.class);
		job1.setOutputValueClass(Vertex.class);
		job1.setMapOutputKeyClass(LongWritable.class);
		job1.setMapOutputValueClass(Vertex.class);   
		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
	//	job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job1,new Path(args[0]));
		FileOutputFormat.setOutputPath(job1,new Path(args[1]+"/f0"));

		job1.waitForCompletion(true);
		
		for ( short i = 0; i < 5; i++ ) {
			/* ... Second Map-Reduce job to propagate the group number */
			Job job2 = Job.getInstance();
			job2.setJobName("Job2");

			job2.setJarByClass(Graph.class);
			job2.setOutputKeyClass(LongWritable.class);
			job2.setOutputValueClass(Vertex.class);
			job2.setMapOutputKeyClass(LongWritable.class);
			job2.setMapOutputValueClass(Vertex.class);   
			
			job2.setMapperClass(Mapper2.class);
			job2.setReducerClass(Reducer2.class);
			job2.setInputFormatClass(SequenceFileInputFormat.class);
			job2.setOutputFormatClass(SequenceFileOutputFormat.class);

			FileInputFormat.setInputPaths(job2,new Path(args[1]+"/f"+i));
			FileOutputFormat.setOutputPath(job2,new Path(args[1]+"/f"+(i+1)));

			job2.waitForCompletion(true);
		}

		        Job job3 = Job.getInstance();
		        job3 = Job.getInstance();
		        /* ... Final Map-Reduce job to calculate the connected component sizes */
		    	
		        job3.setJobName("Job3");
		
		        job3.setJarByClass(Graph.class);
		        job3.setOutputKeyClass(LongWritable.class);
		        job3.setOutputValueClass(IntWritable.class);
		        job3.setMapOutputKeyClass(LongWritable.class);
		        job3.setMapOutputValueClass(IntWritable.class);   
		    	job3.setMapperClass(Mapper3.class);
				job3.setReducerClass(Reducer3.class);  
		        job3.setInputFormatClass(SequenceFileInputFormat.class);
		        job3.setOutputFormatClass(TextOutputFormat.class);
		          
		        FileInputFormat.setInputPaths(job3,new Path(args[1]+"/f5"));
		        FileOutputFormat.setOutputPath(job3,new Path(args[2]));
		          
		        job3.waitForCompletion(true);
		        job3.waitForCompletion(true);
	        

	}
}
