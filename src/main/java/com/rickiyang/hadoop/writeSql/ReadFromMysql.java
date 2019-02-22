package com.rickiyang.hadoop.writeSql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ReadFromMysql {
    public static class EmpWritable implements Writable, DBWritable {
        private Integer empno;
        private String ename;
        private String mgr;
        private Double comm;
        private Integer deptno;

        public Integer getEmpno() {
            return empno;
        }

        public void setEmpno(Integer empno) {
            this.empno = empno;
        }

        public String getEname() {
            return ename;
        }

        public void setEname(String ename) {
            this.ename = ename;
        }

        public String getMgr() {
            return mgr;
        }

        public void setMgr(String mgr) {
            this.mgr = mgr;
        }

        public Double getComm() {
            return comm;
        }

        public void setComm(Double comm) {
            this.comm = comm;
        }

        public Integer getDeptno() {
            return deptno;
        }

        public void setDeptno(Integer deptno) {
            this.deptno = deptno;
        }

        @Override
        public String toString() {
            return "empno=" + empno + ", ename=" + ename + ", mgr=" + mgr + ", comm=" + comm + ", deptno="
                    + deptno;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(this.empno);
            out.writeUTF(this.ename);
            out.writeUTF(this.mgr);
            out.writeDouble(this.comm);
            out.writeInt(this.deptno);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.empno = in.readInt();
            this.ename = in.readUTF();
            this.mgr = in.readUTF();
            this.comm = in.readDouble();
            this.deptno = in.readInt();
        }

        @Override
        public void write(PreparedStatement statement) throws SQLException {
            statement.setInt(1, this.empno);
            statement.setString(2, this.ename);
            statement.setString(3, this.mgr);
            statement.setDouble(4, this.comm);
            statement.setInt(5, this.deptno);
        }

        @Override
        public void readFields(ResultSet resultSet) throws SQLException {
            this.empno = resultSet.getInt("empno");
            this.ename = resultSet.getString("ename");
            this.mgr = resultSet.getString("mgr");
            this.comm = resultSet.getDouble("comm");
            this.deptno = resultSet.getInt("deptno");

        }
    }

    public static class ReadFromMysqlMap extends Mapper<LongWritable, EmpWritable, Text, NullWritable> {
        private Text outputKey = new Text();
        private NullWritable outputValue = NullWritable.get();

        @Override
        protected void map(LongWritable key, EmpWritable value,
                           Mapper<LongWritable, EmpWritable, Text, NullWritable>.Context context)
                throws IOException, InterruptedException {
            outputKey.set(value.toString());
            context.write(outputKey, outputValue);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //设置数据库连接
        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/emp", "root", "123456");
        Job job = Job.getInstance(conf);
        job.setJarByClass(ReadFromMysql.class);
        job.setJobName("读MySQL");

        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(DBInputFormat.class);
        //DBInputFormat.setInput(job, inputClass, tableName, conditions, orderBy, fieldNames);
        DBInputFormat.setInput(job, EmpWritable.class, "select * from t_emp", "select count(1) from t_emp");

        Path outputDir = new Path("/bd32/readfrommysql");
        outputDir.getFileSystem(conf).delete(outputDir, true);
        FileOutputFormat.setOutputPath(job, outputDir);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
