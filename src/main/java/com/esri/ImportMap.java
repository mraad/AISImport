package com.esri;

import com.esri.dbf.DBFReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
final class ImportMap extends Mapper<LongWritable, Text, DateHour, Text>
{
    private final TimeZone m_timeZone = TimeZone.getTimeZone("UTC");
    private final SimpleDateFormat m_origDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private final SimpleDateFormat m_destDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    private final ByteArrayOutputStream m_byteArrayOutputStream = new ByteArrayOutputStream(4096);
    private final PrintWriter m_printWriter = new PrintWriter(m_byteArrayOutputStream);
    private final FastTok m_fastTok = new FastTok();
    private final DateHour m_key = new DateHour();
    private final Text m_val = new Text();
    private final Calendar m_calendar = Calendar.getInstance();
    private final Map<Long, Long> m_voyageId2Draught = new HashMap<Long, Long>();
    private Pattern m_patternLine = Pattern.compile("^\\((\\-\\d+\\.\\d+), (\\d+\\.\\d+)\\)$");
    private Matcher m_matcherLine = m_patternLine.matcher("dummy");

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException
    {
        super.setup(context);

        m_origDateFormat.setTimeZone(m_timeZone);
        m_destDateFormat.setTimeZone(m_timeZone);
        m_calendar.setTimeZone(m_timeZone);

        readVoyage(context.getConfiguration());
    }

    private void readVoyage(final Configuration configuration) throws IOException
    {
        final String name = configuration.get("com.esri.voyage", "voyage.dbf");
        final String voyageIdKey = configuration.get("com.esri.voyageID", "VoyageID");
        final String draughtKey = configuration.get("com.esri.draught", "Draught");
        final Path path = new Path(name);
        final FSDataInputStream dataInputStream = path.getFileSystem(configuration).open(path);
        try
        {
            final Map<String, Object> map = new HashMap<String, Object>();
            final DBFReader dbfReader = new DBFReader(dataInputStream);
            while (dbfReader.readRecordAsMap(map) != null)
            {
                final Long voyageId = (Long) map.get(voyageIdKey);
                final Long draught = (Long) map.get(draughtKey);
                m_voyageId2Draught.put(voyageId, draught);
            }
        }
        finally
        {
            dataInputStream.close();
        }
    }

    @Override
    protected void map(
            final LongWritable key,
            final Text value,
            final Context context) throws IOException, InterruptedException
    {
        final int count = m_fastTok.tokenize(value.toString(), '\t');
        if (count != 12)
        {
            context.getCounter("ImportMap", "Invalid Token Count").increment(1L);
        }
        else
        {
            try
            {
                final String[] tokens = m_fastTok.tokens;

                final Matcher matcher = m_matcherLine.reset(tokens[1]);
                if (matcher.matches())
                {
                    final Date utcDate = m_origDateFormat.parse(tokens[6]);
                    m_calendar.setTimeInMillis(utcDate.getTime());
                    tokens[7] = m_destDateFormat.format(utcDate);

                    final long voyageId = Long.parseLong(tokens[8]);
                    final Long draught = m_voyageId2Draught.get(voyageId);

                    m_byteArrayOutputStream.reset();

                    m_printWriter.append(tokens[0]).append('\t');
                    m_printWriter.append(matcher.group(1)).append('\t');
                    m_printWriter.append(matcher.group(2)).append('\t');
                    m_printWriter.append(tokens[2]).append('\t');
                    m_printWriter.append(tokens[3]).append('\t');
                    m_printWriter.append(tokens[4]).append('\t');
                    m_printWriter.append(tokens[5]).append('\t');
                    m_printWriter.append(tokens[6]).append('\t');
                    m_printWriter.append(tokens[7]).append('\t');
                    m_printWriter.append(tokens[8]).append('\t');
                    m_printWriter.append(tokens[9]).append('\t');
                    m_printWriter.append(tokens[10]).append('\t');
                    m_printWriter.append(tokens[11]).append('\t');
                    m_printWriter.print(draught == null ? 0L : draught);
                    m_printWriter.flush();

                    m_val.set(m_byteArrayOutputStream.toByteArray());

                    m_key.yy = m_calendar.get(Calendar.YEAR);
                    m_key.mm = m_calendar.get(Calendar.MONTH) + 1;
                    m_key.dd = m_calendar.get(Calendar.DAY_OF_MONTH);
                    m_key.hh = m_calendar.get(Calendar.HOUR_OF_DAY);
                    context.write(m_key, m_val);
                }
                else
                {
                    context.getCounter("ImportMap", "No Match").increment(1L);
                }
            }
            catch (Throwable t)
            {
                context.getCounter("ImportMap", "Parsing Error").increment(1L);
            }
        }
    }
}
