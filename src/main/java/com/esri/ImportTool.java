package com.esri;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 */
public class ImportTool extends Configured implements Tool
{
    public static void main(final String[] args) throws Exception
    {
        System.exit(ToolRunner.run(new ImportTool(), args));
    }

    @Override
    public int run(final String[] args) throws Exception
    {
        final int rc;

        if (args.length != 2)
        {
            System.err.println("Usage: hadoop jar <jarfile> [generic options] <input> <output>");
            rc = -1;
        }
        else
        {
            final Job job = Job.getInstance(getConf(), ImportTool.class.getSimpleName());

            final Configuration configuration = job.getConfiguration();

            job.setJarByClass(ImportTool.class);

            job.setMapperClass(ImportMap.class);
            job.setMapOutputKeyClass(DateHour.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(ImportReduce.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            job.setNumReduceTasks(24); // TODO - Make configurable

            FileInputFormat.setInputPaths(job, new Path(args[0]));

            final Path outputDir = new Path(args[1]);
            outputDir.getFileSystem(configuration).delete(outputDir, true);
            FileOutputFormat.setOutputPath(job, outputDir);
            LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

            rc = job.waitForCompletion(true) ? 0 : 1;
        }
        return rc;
    }

}
