package com.neusoft.mr;

import java.io.IOException;

import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Reducer;

public class ReduceClass extends MapReduceBase implements Reducer<Text, LongWritable,Text, LongWritable> 
    {
        public void reduce(Text key, Iterator<LongWritable> values,
                OutputCollector<Text, LongWritable> output, Reporter reporter)throws IOException
        {
            Text newkey = new Text();
            newkey.set(key.toString().substring(key.toString().indexOf("::")+1));
            LongWritable result = new LongWritable();
            long tmp = 0;
            int counter = 0;
            while(values.hasNext())//�ۼ�ͬһ��key��ͳ�ƽ��
            {
                tmp = tmp + values.next().get();
                
                counter = counter +1;//���Ĵ���̫�ã�JobTracker��ʱ��û���յ��������ΪTaskTracker�Ѿ�ʧЧ����˶�ʱ����һ��
                if (counter == 1000)
                {
                    counter = 0;
                    reporter.progress();
                }
            }
            result.set(tmp);
            output.collect(newkey, result);//������Ļ��ܽ��
        }    
    }
