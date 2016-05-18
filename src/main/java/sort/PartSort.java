package sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
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


public class PartSort {
    //�Լ������key��Ӧ��ʵ��WritableComparable�ӿ�
    public static class VehiclePair implements WritableComparable<VehiclePair> {
        String IDcomplex;
        String timeStamp;

        /**
         * Set the left and right values.
         */
        public void set(String ID, String ts) {
            this.IDcomplex = ID;
            this.timeStamp = ts;
        }

        public String getID() {
            return IDcomplex;
        }

        public String getTimeStamp() {
            return timeStamp;
        }

        //�����л��������еĶ�����ת����IntPair
        public void readFields(DataInput in) throws IOException {
            // TODO Auto-generated method stub
            IDcomplex = in.readUTF();
            timeStamp = in.readUTF();
        }

        //���л�����IntPairת����ʹ�������͵Ķ�����
        public void write(DataOutput out) throws IOException {
            // TODO Auto-generated method stub
            out.writeUTF(IDcomplex);
            out.writeUTF(timeStamp);
        }

        //key�ıȽ�
        public int compareTo(VehiclePair o) {
            // TODO Auto-generated method stub
            if (!IDcomplex.equals(o.IDcomplex)) {
                return IDcomplex.compareTo(o.IDcomplex) < 0 ? -1 : 1;
            } else if (!timeStamp.equals(o.timeStamp)) {
                return timeStamp.compareTo(o.timeStamp) < 0 ? -1 : 1;
            } else {
                return 0;
            }
        }

        //�¶�����Ӧ����д����������
        @Override
        //The hashCode() method is used by the HashPartitioner (the default partitioner in MapReduce)
        public int hashCode() {
            int ch = IDcomplex.hashCode();
            return ch * ch + timeStamp.hashCode();
        }

        @Override
        public boolean equals(Object _pair) {
            if (_pair == null)
                return false;
            if (this == _pair)
                return true;
            if (_pair instanceof VehiclePair) {
                VehiclePair r = (VehiclePair) _pair;
                return r.IDcomplex.equals(IDcomplex) && r.timeStamp.equals(timeStamp);
            } else {
                return false;
            }
        }
    }

    /**
     * ���������ࡣ����firstȷ��Partition��
     */
    public static class FirstPartitioner extends Partitioner<VehiclePair, Text> {
        @Override
        public int getPartition(VehiclePair key, Text value, int numPartitions) {
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
    public static class GroupingComparator extends WritableComparator {
        protected GroupingComparator() {
            super(VehiclePair.class, true);
        }

        @Override
        //Compare two WritableComparables.
        public int compare(WritableComparable w1, WritableComparable w2) {
            VehiclePair vp1 = (VehiclePair) w1;
            VehiclePair vp2 = (VehiclePair) w2;
            String l = vp1.getID();
            String r = vp2.getID();
            return l.equals(r) ? 0 : (l.compareTo(r) < 0 ? -1 : 1);
        }
    }


    // �Զ���map
    public static class Map extends Mapper<LongWritable, Text, VehiclePair, Text> {
        private final VehiclePair vehicleKey = new VehiclePair();
        private final Text textValue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] lss = line.split(",");
            if (lss.length >= 18) {
                StringBuffer ID = new StringBuffer(lss[0]);
                StringBuffer text = new StringBuffer(lss[6]);
                for (int i = 1; i < 6; i++) ID.append("," + lss[i]);
                for (int i = 7; i < lss.length; i++) text.append("," + lss[i]);
                String ts = lss[7];
                vehicleKey.set(ID.toString(), ts);
                textValue.set(text.toString());
                context.write(vehicleKey, textValue);
            }
            // < (��ES1965,2,320000,999,320500,0), 320000,2014-05-01...>
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
    public static class Reduce extends Reducer<VehiclePair, Text, Text, Text> {
        private final Text left = new Text();

        //        private static final Text SEPARATOR = new Text("------------------------------------------------");
        //   output :<��ED5683 , values>
        public void reduce(VehiclePair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            context.write(SEPARATOR, null);
            left.set(key.getID());


            //ע��format�ĸ�ʽҪ������String�ĸ�ʽ��ƥ��
            DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try {
                boolean firstFlag = true;
                Date preBackDate = new Date();
                Date preAcceptDate = new Date();
                double preLongtiTude = 0d;
                double preLatiTude = 0d;
                int preerror = 0;
                int precatalogIndex = 1;
                for (Text val : values) {
                    String[] linesItems = val.toString().split(",");
                    String backTimeStr = linesItems[1];
                    String acceptTimeStr = linesItems[2];

                    Date backDate = new Date();
                    backDate = sdf.parse(backTimeStr);
                    Date acceptDate = new Date();
                    acceptDate = sdf.parse(acceptTimeStr);
//                    System.out.println(backDate.toString());
//                    System.out.println(acceptDate.toString());

                    String longTitudeStr = linesItems[3];
                    String latitudeStr = linesItems[4];
                    double longititude = Double.parseDouble(longTitudeStr) / 1000000;
                    double latitude = Double.parseDouble(latitudeStr) / 1000000;

                    int errorData = 0;
                    int catalogIndex = 0;

                    if(firstFlag){
                        if(backDate.getDay() != acceptDate.getDay()){
                            firstFlag = true;
                            errorData = 1;
                            catalogIndex = 0;
                        } else{
                            // ��һ����Ч������
                            preBackDate = backDate;
                            preAcceptDate = acceptDate;
                            preLongtiTude = longititude;
                            preLatiTude = latitude;
                            preerror = 0;
                            precatalogIndex = 1;

                            errorData = 0;
                            catalogIndex = 1;
                            firstFlag = false;
                        }


                    } else {
                        double distance = DistanceP2P.GetDistance(longititude,latitude,preLongtiTude,preLatiTude)/1000;
                        double interval = (backDate.getTime() - preBackDate.getTime())/1000;

                        if(preBackDate == backDate && (preLatiTude != latitude || preLongtiTude != longititude)){
                           // ʱ�䲻��,����任,�쳣
                            errorData = 1;
                            catalogIndex = 0;
                        } else if(backDate.getDay() != acceptDate.getDay()){
                            // ����ʱ�̺ͽ���ʱ�̲���ͬһ��,�쳣
                            errorData = 1;
                            catalogIndex = 0;
//                        } else if(largerThanFiveMinute(acceptDate,backDate)){
//                            // �ش����ڴ���5����,�µĶ���
//                            catalogIndex = precatalogIndex +1;
//                            errorData = 0;
                        } else if(interval > 5*60){
                            // �����������5����,�µĶ���
                            catalogIndex = precatalogIndex +1;
                            errorData = 0;
                        } else if(distance>10){
                            // ��γ�Ⱦ������10km
                           catalogIndex = precatalogIndex +1;
                            errorData = 0;

                        } else if(distance*3600 / interval > 120){
                            //��γ�Ⱦ���/�����������120km/h
                            catalogIndex = precatalogIndex +1;
                            errorData = 0;
                        } else{
                            //ͬһ��
                            errorData = 0;
                            catalogIndex = precatalogIndex;

                            //����pre, �Ա���һ�ֵ���
                            preBackDate = backDate;
                            preAcceptDate = acceptDate;
                            preLongtiTude = longititude;
                            preLatiTude = latitude;
                            preerror = 0;
                            precatalogIndex = catalogIndex;

                        }

                        if(errorData != 1){
                            //����pre, �Ա���һ�ֵ���
                            preBackDate = backDate;
                            preAcceptDate = acceptDate;
                            preLongtiTude = longititude;
                            preLatiTude = latitude;
                            preerror = 0;
                            precatalogIndex = catalogIndex;
                        }
                    }

                    val.set(val.toString()+","+errorData+","+catalogIndex+","+longititude+","+latitude);
                    context.write(left, val);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private  static  boolean largerThanFiveMinute(Date d1,Date d2){
        if((d1.getTime() - d2.getTime())/1000 > 5*60){
            return true;
        }
        return false;
    }

    /**
     * @param args
     * @throws URISyntaxException
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        // TODO Auto-generated method stub
        // ��ȡhadoop����
        if (args.length < 4) {
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
        for (Path path : paths) {
            FileInputFormat.addInputPath(job, path);
        }
        // ���hdfs·��
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        // �ύjob
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
