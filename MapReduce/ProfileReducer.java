import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ProfileReducer
                extends Reducer<NullWritable, Text, NullWritable, Text> {
        @Override
        public void reduce(NullWritable key, Iterable<Text> values, Context context)
                        throws IOException, InterruptedException {
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

                // This profile job calculate the average of the following numerical columns (to
                // use it, uncomment the code below)):
                // numerical columns: 8, 10, 13, 14
                /*
                 * int sumDepDelay = 0;
                 * int sumArrDelay = 0;
                 * int sumAirTime = 0;
                 * int sumDistance = 0;
                 * int count = 0;
                 * for (Text val : values) {
                 * String[] columns = val.toString().split(",");
                 * sumDepDelay += Integer.parseInt(columns[0]);
                 * sumArrDelay += Integer.parseInt(columns[1]);
                 * sumAirTime += Integer.parseInt(columns[2]);
                 * sumDistance += Integer.parseInt(columns[3]);
                 * count++;
                 * }
                 * 
                 * double avgDepDelay = (double) sumDepDelay / count;
                 * double avgArrDelay = (double) sumArrDelay / count;
                 * double avgAirTime = (double) sumAirTime / count;
                 * double avgDistance = (double) sumDistance / count;
                 * 
                 * String numericalValues = avgDepDelay + "," + avgArrDelay + "," + avgAirTime +
                 * "," + avgDistance;
                 * 
                 * context.write(NullWritable.get(), new Text(numericalValues));
                 */

                // This profile job find the min and max of the following numerical columns (to
                // use it, uncomment the code below)):
                // numerical columns: 8, 10, 13, 14
                /*
                 * int minDepDelay = Integer.MAX_VALUE;
                 * int maxDepDelay = Integer.MIN_VALUE;
                 * int minArrDelay = Integer.MAX_VALUE;
                 * int maxArrDelay = Integer.MIN_VALUE;
                 * int minAirTime = Integer.MAX_VALUE;
                 * int maxAirTime = Integer.MIN_VALUE;
                 * int minDistance = Integer.MAX_VALUE;
                 * int maxDistance = Integer.MIN_VALUE;
                 * 
                 * for (Text val : values) {
                 * String[] columns = val.toString().split(",");
                 * int depDelay = Integer.parseInt(columns[0]);
                 * int arrDelay = Integer.parseInt(columns[1]);
                 * int airTime = Integer.parseInt(columns[2]);
                 * int distance = Integer.parseInt(columns[3]);
                 * 
                 * if (depDelay < minDepDelay) {
                 * minDepDelay = depDelay;
                 * }
                 * if (depDelay > maxDepDelay) {
                 * maxDepDelay = depDelay;
                 * }
                 * if (arrDelay < minArrDelay) {
                 * minArrDelay = arrDelay;
                 * }
                 * if (arrDelay > maxArrDelay) {
                 * maxArrDelay = arrDelay;
                 * }
                 * if (airTime < minAirTime) {
                 * minAirTime = airTime;
                 * }
                 * if (airTime > maxAirTime) {
                 * maxAirTime = airTime;
                 * }
                 * if (distance < minDistance) {
                 * minDistance = distance;
                 * }
                 * if (distance > maxDistance) {
                 * maxDistance = distance;
                 * }
                 * }
                 * 
                 * String numericalValues = minDepDelay + "," + maxDepDelay + "," + minArrDelay
                 * + ","
                 * + maxArrDelay + "," + minAirTime + "," + maxAirTime + ","
                 * + minDistance + "," + maxDistance;
                 * 
                 * context.write(NullWritable.get(), new Text(numericalValues));
                 */

                // This profile job count the times of flight delay due to different reasons of
                // the following categorical columns (to use it, uncomment the
                // code below)):
                // categorical columns (or the columns that can be used as categorical
                // variables): 10, 12, 15, 16, 17, 18, 19
                /*
                 * int countCarrierDelay = 0;
                 * int countWeatherDelay = 0;
                 * int countNASDelay = 0;
                 * int countSecurityDelay = 0;
                 * int countLateAircraftDelay = 0;
                 * int countOtherDelay = 0;
                 * int totalDelay = 0;
                 * int countCancelled = 0;
                 * int countAheadOrOnTime = 0;
                 * 
                 * for (Text val : values) {
                 * String[] columns = val.toString().split(",");
                 * int delay = Integer.parseInt(columns[0]);
                 * int cancelled = Integer.parseInt(columns[1]);
                 * int carrierDelay = Integer.parseInt(columns[2]);
                 * int weatherDelay = Integer.parseInt(columns[3]);
                 * int NASDelay = Integer.parseInt(columns[4]);
                 * int securityDelay = Integer.parseInt(columns[5]);
                 * int lateAircraftDelay = Integer.parseInt(columns[6]);
                 * 
                 * if (cancelled > 0) {
                 * countCancelled++;
                 * continue;
                 * }
                 * 
                 * if (delay <= 0) {
                 * countAheadOrOnTime++;
                 * continue;
                 * } else {
                 * if (carrierDelay > 0) {
                 * countCarrierDelay++;
                 * }
                 * if (weatherDelay > 0) {
                 * countWeatherDelay++;
                 * }
                 * if (NASDelay > 0) {
                 * countNASDelay++;
                 * }
                 * if (securityDelay > 0) {
                 * countSecurityDelay++;
                 * }
                 * if (lateAircraftDelay > 0) {
                 * countLateAircraftDelay++;
                 * }
                 * if (carrierDelay == 0 && weatherDelay == 0 && NASDelay == 0
                 * && securityDelay == 0 && lateAircraftDelay == 0) {
                 * countOtherDelay++;
                 * }
                 * totalDelay++;
                 * }
                 * 
                 * }
                 */
                int countDelay = 0;
                int countCancelled = 0;
                int countAheadOrOnTime = 0;
                int countTotal = 0;

                for (Text val : values) {
                        String[] columns = val.toString().split(",");
                        if (columns[0].toString().equals("B6")) {
                                if (Integer.parseInt(columns[1]) <= 0) {
                                        countAheadOrOnTime++;
                                } else {
                                        countDelay++;
                                }
                                if (Integer.parseInt(columns[2]) > 0) {
                                        countCancelled++;
                                }
                                countTotal++;
                        }
                }

                String categoricalValues = countDelay + "," + countCancelled + "," + countAheadOrOnTime + ","
                                + countTotal;

                context.write(NullWritable.get(), new Text(categoricalValues));

        }
}