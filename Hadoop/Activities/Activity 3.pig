inputFile = LOAD 'hdfs:///user/shravyakumar/sales.csv' USING PigStorage(',') AS (Product:chararray,Price:chararray,Payment_Type:chararray,Name:chararray,City:chararray,State:chararray,Country:chararray);
grpd = GROUP inputFile BY Country;
result = FOREACH grpd GENERATE CONCAT((chararray)$0, CONCAT(': ', (chararray)COUNT($1)));
STORE result INTO 'hdfs:///user/shravyakumar/PigOutput';