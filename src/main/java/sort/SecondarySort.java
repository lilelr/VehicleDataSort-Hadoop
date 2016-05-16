package sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

//http://blog.csdn.net/heyutao007/article/details/5890103
public class SecondarySort {
    //�Լ������key��Ӧ��ʵ��WritableComparable�ӿ�
    public static class IntPair implements WritableComparable<IntPair> {
        int first;
        int second;

        /**
         * Set the left and right values.
         */
        public void set(int left, int right) {
            first = left;
            second = right;
        }

        public int getFirst() {
            return first;
        }

        public int getSecond() {
            return second;
        }


        //�����л��������еĶ�����ת����IntPair
        public void readFields(DataInput in) throws IOException {
            // TODO Auto-generated method stub
            first = in.readInt();
            second = in.readInt();
        }


        //���л�����IntPairת����ʹ�������͵Ķ�����
        public void write(DataOutput out) throws IOException {
            // TODO Auto-generated method stub
            out.writeInt(first);
            out.writeInt(second);
        }

        //key�ıȽ�
        public int compareTo(IntPair o) {
            // TODO Auto-generated method stub
            if (first != o.first) {
                return first < o.first ? -1 : 1;
            } else if (second != o.second) {
                return second < o.second ? -1 : 1;
            } else {
                return 0;
            }
        }

        //�¶�����Ӧ����д����������
        @Override
        //The hashCode() method is used by the HashPartitioner (the default partitioner in MapReduce)
        public int hashCode() {
            return first * 157 + second;
        }

        @Override
        public boolean equals(Object right) {
            if (right == null)
                return false;
            if (this == right)
                return true;
            if (right instanceof IntPair) {
                IntPair r = (IntPair) right;
                return r.first == first && r.second == second;
            } else {
                return false;
            }
        }
    }

    /**
     * ���������ࡣ����firstȷ��Partition��
     */
    public static class FirstPartitioner extends Partitioner<IntPair, IntWritable> {
        @Override
        public int getPartition(IntPair key, IntWritable value, int numPartitions) {
            return Math.abs(key.getFirst() * 127) % numPartitions;
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
    public static class GroupingComparator extends WritableComparator {
        protected GroupingComparator() {
            super(IntPair.class, true);
        }

        @Override
        //Compare two WritableComparables.
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntPair ip1 = (IntPair) w1;
            IntPair ip2 = (IntPair) w2;
            int l = ip1.getFirst();
            int r = ip2.getFirst();
            return l == r ? 0 : (l < r ? -1 : 1);
        }
    }


    // �Զ���map
    public static class Map extends Mapper<LongWritable, Text, IntPair, IntWritable> {
        private final IntPair intkey = new IntPair();
        private final IntWritable intvalue = new IntWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            int left = 0;
            int right = 0;
            if (tokenizer.hasMoreTokens()) {
                left = Integer.parseInt(tokenizer.nextToken());
                if (tokenizer.hasMoreTokens())
                    right = Integer.parseInt(tokenizer.nextToken());
                intkey.set(left, right);
                intvalue.set(right);
                context.write(intkey, intvalue);
            }
        }
    }

    // �Զ���reduce
    //
    public static class Reduce extends Reducer<IntPair, IntWritable, Text, IntWritable> {
        private final Text left = new Text();
        private static final Text SEPARATOR = new Text("------------------------------------------------");

        public void reduce(IntPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            context.write(SEPARATOR, null);
            left.set(Integer.toString(key.getFirst()));
            for (IntWritable val : values) {
                context.write(left, val);
            }
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO Auto-generated method stub
        // ��ȡhadoop����
        Configuration conf = new Configuration();
        // ʵ����һ����ҵ
        Job job = Job.getInstance(conf, "secondarysort");
        job.setJarByClass(SecondarySort.class);
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
        job.setMapOutputKeyClass(IntPair.class);
        // map���Value������
        job.setMapOutputValueClass(IntWritable.class);
        // rduce���Key�����ͣ���Text����Ϊʹ�õ�OutputFormatClass��TextOutputFormat
        job.setOutputKeyClass(Text.class);
        // rduce���Value������
        job.setOutputValueClass(IntWritable.class);

        // ����������ݼ��ָ��С���ݿ�splites��ͬʱ�ṩһ��RecordReder��ʵ�֡�
        job.setInputFormatClass(TextInputFormat.class);
        // �ṩһ��RecordWriter��ʵ�֣��������������
        job.setOutputFormatClass(TextOutputFormat.class);

        // ����hdfs·��
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // ���hdfs·��
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // �ύjob
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
