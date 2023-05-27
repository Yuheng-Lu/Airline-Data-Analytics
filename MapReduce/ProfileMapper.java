import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProfileMapper
        extends Mapper<LongWritable, Text, NullWritable, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        int columnNumber = 31;
        String[] columns = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        /*
         * 0: Reporting_Airline
         * 1: Flight_Number_Reporting_Airline
         * 2: Origin
         * 3: OriginCityName
         * 4: OriginState
         * 5: Dest
         * 6: DestCityName
         * 7: DestState
         * 8: DepDelay
         * 9: DepDel15
         * 10: ArrDelay
         * 11: ArrDel15
         * 12: Cancelled
         * 13: AirTime
         * 14: Distance
         * 15: CarrierDelay
         * 16: WeatherDelay
         * 17: NASDelay
         * 18: SecurityDelay
         * 19: LateAircraftDelay
         */

        // This profile job output the following numerical columns (to
        // use it, uncomment the code below)):
        // numerical columns: 8, 10, 13, 14
        /*
         * int[] numericalColumns = { 8, 10, 13, 14 };
         * String numericalValues = "";
         * for (int i = 0; i < numericalColumns.length; i++) {
         * numericalValues += columns[numericalColumns[i]] + ",";
         * }
         * context.write(NullWritable.get(), new Text(numericalValues));
         */
        // This profile job output the following categorical columns (to
        // use it, uncomment the code below)):
        // categorical columns (or the columns that can be used as categorical
        // variables): 10, 12, 15, 16, 17, 18, 19

        int[] categoricalColumns = { 0, 10, 12 };
        String categoricalValues = "";
        for (int i = 0; i < categoricalColumns.length; i++) {
            categoricalValues += columns[categoricalColumns[i]] + ",";
        }
        context.write(NullWritable.get(), new Text(categoricalValues));
    }
}