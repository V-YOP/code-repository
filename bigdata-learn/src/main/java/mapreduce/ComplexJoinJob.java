package mapreduce;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringJoiner;


/*
需求：现在有一张部门表和一张雇员表，其中雇员属于特定部门，
且有自己的工资，现在要求获取每个部门的工资最大的雇员的信息
各表的数据为CSV格式

---- dept.sql ----

CREATE TABLE IF NOT EXISTS dept (
	deptno INT,
	dname STRING,
	loc INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

---- dept.txt ----

10,ACCOUNTING,1700
20,RESEARCH,1800
30,SALES,1900
40,OPERATIONS,1700

---- emp.sql ----

CREATE TABLE IF NOT EXISTS emp (
	empno INT,
	ename STRING,
	job STRING,
	mgr INT, -- 上级
	hiredate STRING, -- 入职时间
	sal DOUBLE, -- 薪水
	comm DOUBLE, -- 奖金
	deptno INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

---- emp.txt ----

7369,SMITH,CLERK,7902,1980-12-17,800.00,,20
7499,ALLEN,SALESMAN,7698,1981-2-20,1600.00,300.00,30
7521,WARD,SALESMAN,7698,1981-2-22,1250.00,500.00,30
7566,JONES,MANAGER,7839,1981-4-2,2975.00,,20
7654,MARTIN,SALESMAN,7698,1981-9-28,1250.00,1400.00,30
7698,BLAKE,MANAGER,7839,1981-5-1,2850,,30
7782,CLARK,MANAGER,7839,1981-6-9,2450.00,,10
7788,SCOTT,ANALYST,7566,1987-4-19,3000.00,,20
7839,KING,PRESIDENT,,1981-11-17,5000.00,,10
7844,TURNER,SALESMAN,7698,1981-9-8,1500.00,0.00,30
7876,ADAMS,CLERK,7788,1987-5-23,1100.00,,20
7900,JAMES,CLERK,7698,1981-12-3,950.00,,30
7902,FORD,ANALYST,7566,1981-12-3,3000.00,,20
7934,MILLER,CLERK,7782,1982-1-23,1300.00,,10
*/
// 注意这个需要两个输入，第一个输入是dept，第二个输入是emp
public class ComplexJoinJob {

    // Mapper输出的key
    public static class CombineKey implements WritableComparable<CombineKey> {
        private Integer deptno;
        private Integer virtualKey;
        @Override
        public int compareTo(CombineKey o) {
            if (!deptno.equals(o.deptno))
                return deptno.compareTo(o.deptno);
            return virtualKey.compareTo(o.virtualKey);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(deptno);
            out.writeInt(virtualKey);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            deptno = in.readInt();
            virtualKey = in.readInt();
        }
    }

    public static class DeptMapper extends Mapper<LongWritable, Text, CombineKey, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] cols = value.toString().split(",");
            int deptno = Integer.parseInt(cols[0]);
            CombineKey outputK = new CombineKey();
            outputK.deptno = deptno;
            outputK.virtualKey = 0;
            context.write(outputK, value);
        }
    }

    public static class EmpMapper extends Mapper<LongWritable, Text, CombineKey, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] cols = value.toString().split(",");
            int deptno = Integer.parseInt(cols[7]);
            CombineKey outputK = new CombineKey();
            outputK.deptno = deptno;
            outputK.virtualKey = 1;
            context.write(outputK, value);
        }
    }

    public static class DeptnoPartitioner extends Partitioner<CombineKey, Text> {
        @Override
        public int getPartition(CombineKey combineKey, Text text, int numPartitions) {
            return (combineKey.deptno.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class DeptnoGroupComparator extends WritableComparator {
        // 永远记着定义Comparator的时候别把这茬忘掉了……
        public DeptnoGroupComparator() {
            super(CombineKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((CombineKey) a).deptno.compareTo(((CombineKey) b).deptno);
        }
    }

    // Combiner的职责是在每个Mapper本地聚集
    // 对于DeptMapper，没有聚集的必要，对于EmpMapper，可以直接找到该Mapper该部门最大工资的雇员，只写它就行
    public static class SomeCombiner extends Reducer<CombineKey, Text, CombineKey, Text> {
        @Override
        protected void reduce(CombineKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (key.virtualKey.equals(0)) {
                for (Text value : values) {
                    context.write(key, value);
                }
                return;
            }

            double maxSal = -1;
            String maxEmp = null;

            for (Text value : values) {
                String emp = value.toString();
                double sal = Double.parseDouble(emp.split(",")[5]);
                if (sal > maxSal) {
                    maxSal = sal;
                    maxEmp = emp;
                }
            }
            context.write(key, new Text(maxEmp));
        }
    }

    public static class SomeReducer extends Reducer<CombineKey, Text, NullWritable, Text> {
        @Override
        protected void reduce(CombineKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 虚拟键不可能是1，如果是1，则说明这些雇员根本没有对应的部门
            // 考虑当前连接是内连接，这时直接返回即可

            if (key.virtualKey.equals(1)) {
                return;
            }

            String[] deptCols = values.iterator().next().toString().split(",");

            double maxSal = -1;
            String maxEmp = null;
            for (Text value : values) {
                String emp = value.toString();
                double sal = Double.parseDouble(emp.split(",")[5]);
                if (sal > maxSal) {
                    maxSal = sal;
                    maxEmp = emp;
                }
            }

            // 部门没有任何雇员，这里是内连接，也是直接返回
            if (maxEmp == null) {
                return;
            }

            String[] empCols = maxEmp.split(",");

            String result = new StringJoiner(",")
                    .add(deptCols[1])
                    .add(empCols[1])
                    .add(empCols[5])
                    .toString();

            context.write(NullWritable.get(), new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(args);
        Job job = Job.getInstance(parser.getConfiguration());
        args = parser.getRemainingArgs();
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(CombineKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(DeptnoPartitioner.class);
        job.setGroupingComparatorClass(DeptnoGroupComparator.class);
        job.setCombinerClass(SomeCombiner.class);
        job.setReducerClass(SomeReducer.class);
        // 诶，就是玩
        job.setNumReduceTasks(2);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, DeptMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, EmpMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
