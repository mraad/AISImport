package com.esri;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.UUID;

/**
 */
final class ImportReduce
        extends Reducer<DateHour, Text, NullWritable, Text>
{
    private MultipleOutputs<NullWritable, Text> m_mos;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException
    {
        m_mos = new MultipleOutputs<NullWritable, Text>(context);
    }

    @Override
    protected void reduce(
            final DateHour key,
            final Iterable<Text> values,
            final Context context) throws IOException, InterruptedException
    {
        final String outputPath = String.format("/ais/%4d/%02d/%02d/%02d/%s",
                key.yy, key.mm, key.dd, key.hh, UUID.randomUUID().toString());
        for (final Text value : values)
        {
            m_mos.write(NullWritable.get(), value, outputPath);
        }
    }

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException
    {
        m_mos.close();
    }
}
