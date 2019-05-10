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
	public long VID; 
	public long size;// the vertex ID
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
	  
	  public void write ( DataOutput out ) throws IOException {
	        out.writeInt(tag);
			out.writeLong(group);
	        out.writeLong(VID);
	        out.writeLong(size);
	        for (int i=0;i<adjacent.size();i++)
	        {
	        	out.writeLong(adjacent.get(i));
	        }
	        
	    }

	    public void readFields ( DataInput in ) throws IOException {
	        tag= in.readInt();
			group = in.readLong();
	        VID = in.readLong();
	        adjacent=new Vector<Long>();
	        size=in.readLong();
	        for (long y=0;y<size;y++)
	        {
	        	adjacent.add(in.readLong());
	        }
	    }
}//end class Vector

public class Graph {

	public static class FirstMapper extends Mapper<Object,Text,LongWritable,Vertex > {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner scanner = new Scanner(value.toString()).useDelimiter(",");
            Vector<Long> adjacent_vector = new Vector<Long>();
            long vid = scanner.nextLong();
            while(scanner.hasNextLong())
			{
				long variable = scanner.nextLong();
				adjacent_vector.addElement(variable);
			}
            context.write(new LongWritable(vid),new Vertex(0,vid,vid,adjacent_vector));
            scanner.close();
        }
    }//end FirstMapper class
	
	 public static class FirstReducer extends Reducer<LongWritable,Vertex,LongWritable,Vertex> {
	        @Override
	        public void reduce ( LongWritable key, Iterable<Vertex> values, Context context )
	                           throws IOException, InterruptedException {
	        	 for (Vertex v:values)
				 {
	           		 context.write(key,new Vertex(v.tag,v.group,v.VID,v.adjacent));						
				  }//end for
				}//end method reduce	
	        }//end class FirstReducer

	 public static class SecondMapper extends Mapper<LongWritable,Vertex,LongWritable,Vertex > 
	 {	
	     @Override       
	     public void map ( LongWritable key, Vertex value, Context context )
	                     throws IOException, InterruptedException {
	         context.write(new LongWritable(value.VID),value);
	         
	         for (long n:value.adjacent)
	 		{
	 			context.write(new LongWritable(n),new Vertex(1,value.group));
	 		}
	     }//end for
	 }//end SecondMapper class

	 public static class SecondReducer extends Reducer<LongWritable,Vertex,LongWritable,Vertex> {
	        @Override
	        public void reduce ( LongWritable key, Iterable<Vertex> values, Context context )
	                           throws IOException, InterruptedException {
	        	long m=Long.MAX_VALUE;
	        	Vertex vertex_clone=new Vertex();
	        	 for (Vertex v:values)
				 {
	        		 if(v.tag == 0)
	        		 {
	        			 vertex_clone=new Vertex(v.tag,v.group,v.VID,v.adjacent);
	        		 }
	           		 /*
	        		 if (v.group < m)
	           		 {
	           			 m=v.group;
	           		 }
	           		 */
	        		 m=Long.min(m,v.group);
	 			  }//end for
	       		 context.write(new LongWritable(m),new Vertex(0,m,vertex_clone.VID,vertex_clone.adjacent));						
	       		}//end method reduce	
	        }//end class SecondReducer

	 public static class ThirdMapper extends Mapper<LongWritable,Vertex,LongWritable,IntWritable > 
		{
	        @Override
	        public void map ( LongWritable key, Vertex value, Context context )
	                        throws IOException, InterruptedException {
	            context.write(new LongWritable(value.group),new IntWritable(1));
	            
	        }
	    }		

	 public static class ThirdReducer extends Reducer<LongWritable,IntWritable,LongWritable,LongWritable> 
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
	 
}
