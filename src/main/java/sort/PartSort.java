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
    //自己定义的key类应该实现WritableComparable接口
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

        //反序列化，从流中的二进制转换成IntPair
        public void readFields(DataInput in) throws IOException {
            // TODO Auto-generated method stub
            IDcomplex = in.readUTF();
            timeStamp = in.readUTF();
        }

        //序列化，将IntPair转化成使用流传送的二进制
        public void write(DataOutput out) throws IOException {
            // TODO Auto-generated method stub
            out.writeUTF(IDcomplex);
            out.writeUTF(timeStamp);
        }

        //key的比较
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

        //新定义类应该重写的两个方法
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
     * 分区函数类。根据first确定Partition。
     */
    public static class FirstPartitioner extends Partitioner<VehiclePair, Text> {
        @Override
        public int getPartition(VehiclePair key, Text value, int numPartitions) {
            return Math.abs(key.getID().hashCode() * 127) % numPartitions;
        }
    }

    /**
     * 分组函数类。只要first相同就属于同一个组。
     */
    /*//第一种方法，实现接口RawComparator
    public static class GroupingComparator implements RawComparator<IntPair> {
        @Override
        public int compare(IntPair o1, IntPair o2) {
            int l = o1.getFirst();
            int r = o2.getFirst();
            return l == r ? 0 : (l < r ? -1 : 1);
        }
        @Override
        //一个字节一个字节的比，直到找到一个不相同的字节，然后比这个字节的大小作为两个字节流的大小比较结果。
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
            // TODO Auto-generated method stub
             return WritableComparator.compareBytes(b1, s1, Integer.SIZE/8,
                     b2, s2, Integer.SIZE/8);
        }
    }*/
    //第二种方法，继承WritableComparator
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


    // 自定义map
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
            // < (苏ES1965,2,320000,999,320500,0), 320000,2014-05-01...>
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

    // 自定义reduce
    //
    public static class Reduce extends Reducer<VehiclePair, Text, Text, Text> {
        private final Text left = new Text();

        //        private static final Text SEPARATOR = new Text("------------------------------------------------");
        //   output :<苏ED5683 , values>
        public void reduce(VehiclePair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            context.write(SEPARATOR, null);
            left.set(key.getID());


            //注意format的格式要与日期String的格式相匹配
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
                            // 第一条有效的数据
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
                           // 时间不变,坐标变换,异常
                            errorData = 1;
                            catalogIndex = 0;
                        } else if(backDate.getDay() != acceptDate.getDay()){
                            // 回显时刻和接受时刻不在同一天,异常
                            errorData = 1;
                            catalogIndex = 0;
//                        } else if(largerThanFiveMinute(acceptDate,backDate)){
//                            // 回传周期大于5分钟,新的对象
//                            catalogIndex = precatalogIndex +1;
//                            errorData = 0;
                        } else if(interval > 5*60){
                            // 采样间隔大于5分钟,新的对象
                            catalogIndex = precatalogIndex +1;
                            errorData = 0;
                        } else if(distance>10){
                            // 经纬度距离大于10km
                           catalogIndex = precatalogIndex +1;
                            errorData = 0;

                        } else if(distance*3600 / interval > 120){
                            //经纬度距离/采样间隔大于120km/h
                            catalogIndex = precatalogIndex +1;
                            errorData = 0;
                        } else{
                            //同一类
                            errorData = 0;
                            catalogIndex = precatalogIndex;

                            //更新pre, 以便下一轮迭代
                            preBackDate = backDate;
                            preAcceptDate = acceptDate;
                            preLongtiTude = longititude;
                            preLatiTude = latitude;
                            preerror = 0;
                            precatalogIndex = catalogIndex;

                        }

                        if(errorData != 1){
                            //更新pre, 以便下一轮迭代
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
        // 读取hadoop配置
        if (args.length < 4) {
            System.err.println("args error.");
            return;
        }
        Configuration conf = new Configuration();
        // 实例化一道作业
        Job job = Job.getInstance(conf, "partsort");
        conf.set("mapred.textoutputformat.separator", ",");
        job.setJarByClass(PartSort.class);
        // Mapper类型
        job.setMapperClass(Map.class);
        // 不再需要Combiner类型，因为Combiner的输出类型<Text, IntWritable>对Reduce的输入类型<IntPair, IntWritable>不适用
        //job.setCombinerClass(Reduce.class);
        // Reducer类型
        job.setReducerClass(Reduce.class);
        // 分区函数
        job.setPartitionerClass(FirstPartitioner.class);
        // 分组函数
        job.setGroupingComparatorClass(GroupingComparator.class);

        // map 输出Key的类型
        job.setMapOutputKeyClass(VehiclePair.class);
        // map输出Value的类型
        job.setMapOutputValueClass(Text.class);
        // rduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat
        job.setOutputKeyClass(Text.class);
        // rduce输出Value的类型
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(24);
        // 将输入的数据集分割成小数据块splites，同时提供一个RecordReder的实现。
        job.setInputFormatClass(TextInputFormat.class);
        // 提供一个RecordWriter的实现，负责数据输出。
        job.setOutputFormatClass(TextOutputFormat.class);
        Path[] paths = FilesList.getList(conf, args[0], args[1], args[2]);
        // 输入hdfs路径
        for (Path path : paths) {
            FileInputFormat.addInputPath(job, path);
        }
        // 输出hdfs路径
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        // 提交job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
