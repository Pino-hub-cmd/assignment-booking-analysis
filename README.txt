# Commercial Booking Analysis



The idea has been to first understand the scope and details of the elaboration.
then i went to define the steps.
1) validate source files, filter in scope data (confirmed bookings, departure from NL, flight KLM)
2) partitioned bookings data based on calendar date (departureDate) in parquet format
3) on partitioned data layer, perform aggregation for each calendar day in scope of the request
4) now the cake should be done, run a query over the aggregated layer
+-------------------+------+-------+----------------+------------+--------------+------------------+
|destination_country|season|weekday|total_passengers|Total_Adults|Total_Children|Age_Average       |
+-------------------+------+-------+----------------+------------+--------------+------------------+
|United Kingdom     |Spring|4      |33137           |30834       |2303          |39.16368845912267 |
|United Kingdom     |Spring|6      |30975           |28811       |2164          |39.29587955455194 |
|United Kingdom     |Spring|3      |29488           |27620       |1868          |39.960925040353196|
|United Kingdom     |Spring|2      |28701           |26933       |1768          |39.19567652647698 |
|United Kingdom     |Spring|5      |27484           |25595       |1889          |39.439905883241266|
|Germany            |Spring|5      |26685           |24875       |1810          |39.67079097409171 |
|France             |Summer|7      |26462           |24382       |2080          |38.98709510200735 |
|United Kingdom     |Spring|1      |22323           |20967       |1356          |39.749847402036906|
|Germany            |Spring|2      |19459           |18219       |1240          |40.09336942146919 |
|United Kingdom     |Spring|7      |19091           |17879       |1212          |39.22935411647384 |
|United Kingdom     |Summer|6      |17714           |16389       |1325          |39.06558505054676 |
|France             |Spring|2      |17362           |16397       |965           |40.209323859349844|
|China              |Spring|1      |16529           |15425       |1104          |38.46014802925557 |
|United States      |Spring|2      |16217           |15451       |766           |40.7432797703776  |
|Germany            |Spring|3      |15819           |14774       |1045          |39.404233662438855|
|France             |Spring|4      |15551           |14593       |958           |40.03917094370857 |
|United Kingdom     |Summer|3      |14972           |13900       |1072          |39.63398760876005 |
|United States      |Spring|3      |14613           |13646       |967           |40.114950855471875|
|United Kingdom     |Summer|2      |14611           |13580       |1031          |39.33391693575227 |
|Germany            |Spring|6      |14400           |13456       |944           |40.13712094718797 |
+-------------------+------+-------+----------------+------------+--------------+------------------+
only showing top 20 rows



to run the solution:
1 clone the solution
2 from terminal located at the project root: python scripts/process_dataTest.py \
                     --src_file_location data/bookings/booking.json \
                     --airport_country_mapping data/airports/airports.dat \
                     --start_date 2019-01-01 \
                     --end_date 2019-01-31 \
                     --processed_output_file output/processed_results \
                     --aggregated_output_file output/aggregated_results

3 Docker, terminal located at the project root: docker build -t assignment-booking-analysis .
4 as soon as the image as been build:  docker run --rm \
                                         -v /Users/pinoparrella/Desktop/KLM/data:/data \
                                         -v /Users/pinoparrella/Desktop/KLM/output:/output \
                                         assignment-booking-analysis \
                                         --src_file_location /data/bookings/booking.json \
                                         --airport_country_mapping /data/airports/airports.dat \
                                         --start_date 2019-01-01 \
                                         --end_date 2019-01-31 \
                                         --processed_output_file /output/processed_results \
                                         --aggregated_output_file /output/aggregated_results



5) regarding using spark structure streaming:
   we may re use the logic but based on much smaller micro batch,
   in the order of seconds, performing same aggregation persisted over a data layer
   which is always up to date to be queried, can explain better in the interview now is late and i am going to sleep.