import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper
        extends Mapper<LongWritable, Text, NullWritable, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] columns = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        StringBuilder selectedColumns = new StringBuilder();
        int[] columnNumbersToSelect = { 6, 10, 14, 15, 16, 23, 24, 25, 31, 33, 42, 44, 47, 52, 54, 56, 57, 58, 59, 60 };
        int existEmpty = 0;
        int isHeader = 0;
        for (int columnNumber : columnNumbersToSelect) {
            // drop rows with empty columns by escaping
            if (columns[columnNumber].equals("")) {
                if (columns[47].replaceAll("\"", "").replaceAll(" ", "").equals("1.00")) {
                    selectedColumns.append("0").append(",");
                    continue;
                } else if (columnNumber != 56 && columnNumber != 57 && columnNumber != 58 && columnNumber != 59
                        && columnNumber != 60) {
                    existEmpty = 1;
                    break;
                } else {
                    selectedColumns.append("0").append(",");
                    continue;
                }
            }

            // drop header by escaping
            if (columnNumber == 6 && columns[columnNumber].contains("Reporting_Airline")) {
                isHeader = 1;
                break;
            }

            // remove double quotes and spaces
            String temp = columns[columnNumber].replaceAll("\"", "").replaceAll(" ", "");

            // replace commas with dashes
            temp = temp.replaceAll(",", "-");

            // parse the string to int if possible
            try {
                temp = (int) Double.parseDouble(temp) + "";
            } catch (NumberFormatException e) {
                // do nothing
            }

            selectedColumns.append(temp).append(",");
        }

        if (existEmpty != 1 && isHeader != 1) {
            selectedColumns.deleteCharAt(selectedColumns.length() - 1); // Remove the last comma
            context.write(NullWritable.get(), new Text(selectedColumns.toString()));
        }
    }
}