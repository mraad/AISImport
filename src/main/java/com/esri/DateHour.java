package com.esri;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 */
public class DateHour implements WritableComparable<DateHour>
{
    public int yy;
    public int mm;
    public int dd;
    public int hh;

    @Override
    public int compareTo(final DateHour that)
    {
        if (this.yy < that.yy)
        {
            return -1;
        }
        if (this.yy > that.yy)
        {
            return 1;
        }
        if (this.mm < that.mm)
        {
            return -1;
        }
        if (this.mm > that.mm)
        {
            return 1;
        }
        if (this.dd < that.dd)
        {
            return -1;
        }
        if (this.dd > that.dd)
        {
            return 1;
        }
        if (this.hh < that.hh)
        {
            return -1;
        }
        if (this.hh > that.hh)
        {
            return 1;
        }
        return 0;
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof DateHour))
        {
            return false;
        }

        final DateHour dateHour = (DateHour) o;

        if (dd != dateHour.dd)
        {
            return false;
        }
        if (hh != dateHour.hh)
        {
            return false;
        }
        if (mm != dateHour.mm)
        {
            return false;
        }
        if (yy != dateHour.yy)
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = yy;
        result = 31 * result + mm;
        result = 31 * result + dd;
        result = 31 * result + hh;
        return result;
    }

    @Override
    public void write(final DataOutput dataOutput) throws IOException
    {
        dataOutput.writeInt(yy);
        dataOutput.writeInt(mm);
        dataOutput.writeInt(dd);
        dataOutput.writeInt(hh);
    }

    @Override
    public void readFields(final DataInput dataInput) throws IOException
    {
        yy = dataInput.readInt();
        mm = dataInput.readInt();
        dd = dataInput.readInt();
        hh = dataInput.readInt();
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("DateHour{");
        sb.append("year=").append(yy);
        sb.append(", month=").append(mm);
        sb.append(", day=").append(dd);
        sb.append(", hour=").append(hh);
        sb.append('}');
        return sb.toString();
    }
}
