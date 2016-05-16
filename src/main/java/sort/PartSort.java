package sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.yarn.util.Times;

public class PartSort
{
    //�Լ������key��Ӧ��ʵ��WritableComparable�ӿ�
    public static class VehiclePair implements WritableComparable<VehiclePair>
    {
        String IDcomplex;
        String timeStamp;
        /**
         * Set the left and right values.
         */
        public void set(String ID, String ts)
        {
            this.IDcomplex = ID;
            this.timeStamp = ts;
        }
        public String getID()
        {
            return IDcomplex;
        }
        public String getTimeStamp()
        {
            return timeStamp;
        }

        //�����л��������еĶ�����ת����IntPair
        public void readFields(DataInput in) throws IOException
        {
            // TODO Auto-generated method stub
            IDcomplex = in.readUTF();
            timeStamp = in.readUTF();
        }
        //���л�����IntPairת����ʹ�������͵Ķ�����
        public void write(DataOutput out) throws IOException
        {
            // TODO Auto-generated method stub
            out.writeUTF(IDcomplex);
            out.writeUTF(timeStamp);
        }

        //key�ıȽ�
        public int compareTo(VehiclePair o)
        {
            // TODO Auto-generated method stub
            if (!IDcomplex.equals(o.IDcomplex))
            {
                return IDcomplex.compareTo(o.IDcomplex) < 0 ? -1 : 1;
            }
            else if (!timeStamp.equals(o.timeStamp))
            {
                return timeStamp.compareTo(o.timeStamp) < 0 ? -1 : 1;
            }
            else
            {
                return 0;
            }
        }

        //�¶�����Ӧ����д����������
        @Override
        //The hashCode() method is used by the HashPartitioner (the default partitioner in MapReduce)
        public int hashCode()
        {
        	int ch = IDcomplex.hashCode();
            return ch * ch + timeStamp.hashCode();
        }
        @Override
        public boolean equals(Object _pair)
        {
            if (_pair == null)
                return false;
            if (this == _pair)
                return true;
            if (_pair instanceof VehiclePair)
            {
            	VehiclePair r = (VehiclePair) _pair;
                return r.IDcomplex.equals(IDcomplex) && r.timeStamp.equals(timeStamp);
            }
            else
            {
                return false;
            }
        }
    }
    /**
      * ���������ࡣ����firstȷ��Partition��
      */
    public static class FirstPartitioner extends Partitioner<VehiclePair, Text>
    {
        @Override
        public int getPartition(VehiclePair key, Text value,int numPartitions)
        {
            return Math.abs(key.getID().hashCode() * 127) % numPartitions;
        }
    }

    /**
     * ���麯���ࡣֻҪfirst��ͬ������ͬһ���顣
     */
    /*//��һ�ַ�����ʵ�ֽӿ�RawComparator
    public static class GroupingComparator implements RawComparator<IntPair> {
        @Override
        public int compare(IntPair o1, IntPair o2) {
            int l = o1.getFirst();
            int r = o2.getFirst();
            return l == r ? 0 : (l < r ? -1 : 1);
        }
        @Override
        //һ���ֽ�һ���ֽڵıȣ�ֱ���ҵ�һ������ͬ���ֽڣ�Ȼ�������ֽڵĴ�С��Ϊ�����ֽ����Ĵ�С�ȽϽ����
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
            // TODO Auto-generated method stub
             return WritableComparator.compareBytes(b1, s1, Integer.SIZE/8,
                     b2, s2, Integer.SIZE/8);
        }
    }*/
    //�ڶ��ַ������̳�WritableComparator
    public static class GroupingComparator extends WritableComparator
    {
        protected GroupingComparator()
        {
            super(VehiclePair.class, true);
        }
        @Override
        //Compare two WritableComparables.
        public int compare(WritableComparable w1, WritableComparable w2)
        {
        	VehiclePair vp1 = (VehiclePair) w1;
        	VehiclePair vp2 = (VehiclePair) w2;
            String l = vp1.getID();
            String r = vp2.getID();
            return l.equals(r) ? 0 : (l.compareTo(r) < 0 ? -1 : 1);
        }
    }


    // �Զ���map
    public static class Map extends Mapper<LongWritable, Text, VehiclePair, Text>
    {
        private final VehiclePair vehicleKey = new VehiclePair();
        private final Text textValue = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            String[] lss = line.split(",");
            if (lss.length >= 18) 
            {
            	StringBuffer ID = new StringBuffer(lss[0]);
                StringBuffer text = new StringBuffer(lss[6]);
            	for (int i = 1; i < 6; i++) ID.append("," + lss[i]);
            	for (int i = 7; i < lss.length; i++) text.append("," + lss[i]);
            	String ts = lss[7];
            	vehicleKey.set(ID.toString(), ts);
            	textValue.set(text.toString());
            	context.write(vehicleKey, textValue);
            }
//            StringTokenizer tokenizer = new StringTokenizer(line);
//            int left = 0;
//            int right = 0;
//            if (tokenizer.hasMoreTokens())
//            {
//                left = Integer.parseInt(tokenizer.nextToken());
//                if (tokenizer.hasMoreTokens())
//                    right = Integer.parseInt(tokenizer.nextToken());
//                intkey.set(left, right);
//                intvalue.set(right);
//                context.write(intkey, intvalue);
//            }
        }
    }
    // �Զ���reduce
    //
    public static class Reduce extends Reducer<VehiclePair, Text, Text, Text>
    {
        private final Text left = new Text();
//        private static final Text SEPARATOR = new Text("------------------------------------------------");
        //   output :<��ED5683 , values>
        public void reduce(VehiclePair key, Iterable<Text> values,Context context) throws IOException, InterruptedException
        {
//            context.write(SEPARATOR, null);
            left.set(key.getID());
            for (Text val : values)
            {
                context.write(left, val);
            }
        }
    }
    /**
     * @param args
     * @throws URISyntaxException 
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException
    {
        // TODO Auto-generated method stub
        // ��ȡhadoop����
    	if (args.length < 4) 
    	{
    		System.err.println("args error.");
    		return;
    	}
        Configuration conf = new Configuration();
        // ʵ����һ����ҵ
        Job job = Job.getInstance(conf, "partsort");
        conf.set("mapred.textoutputformat.separator", ",");
        job.setJarByClass(PartSort.class);
        // Mapper����
        job.setMapperClass(Map.class);
        // ������ҪCombiner���ͣ���ΪCombiner���������<Text, IntWritable>��Reduce����������<IntPair, IntWritable>������
        //job.setCombinerClass(Reduce.class);
        // Reducer����
        job.setReducerClass(Reduce.class);
        // ��������
        job.setPartitionerClass(FirstPartitioner.class);
        // ���麯��
        job.setGroupingComparatorClass(GroupingComparator.class);

        // map ���Key������
        job.setMapOutputKeyClass(VehiclePair.class);
        // map���Value������
        job.setMapOutputValueClass(Text.class);
        // rduce���Key�����ͣ���Text����Ϊʹ�õ�OutputFormatClass��TextOutputFormat
        job.setOutputKeyClass(Text.class);
        // rduce���Value������
        job.setOutputValueClass(Text.class);
        
        job.setNumReduceTasks(24);
        // ����������ݼ��ָ��С���ݿ�splites��ͬʱ�ṩһ��RecordReder��ʵ�֡�
        job.setInputFormatClass(TextInputFormat.class);
        // �ṩһ��RecordWriter��ʵ�֣��������������
        job.setOutputFormatClass(TextOutputFormat.class);
        Path[] paths = FilesList.getList(conf, args[0], args[1], args[2]);
        // ����hdfs·��
        for (Path path : paths){
            FileInputFormat.addInputPath(job, path);
        }
        // ���hdfs·��
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        // �ύjob
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
