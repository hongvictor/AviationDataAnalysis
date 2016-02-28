--------------------------------------------------------------
-- NAME
-- 	   topontimearrival.pig
-- INPUT 
--     $PIG_IN_DIR
-- OUTPUT
--     stout
-- FIELDS
--     AirlineID
--     ArrDel15
--------------------------------------------------------------

in  = LOAD '$PIG_IN_DIR' AS
         ( AirlineID:chararray,
           ArrDel15:int		   
         );
 
group_by_airline = GROUP in BY (AirlineID);
 
average_ontime = FOREACH group_by_airline
                 GENERATE FLATTEN(group) AS (AirlineID),
                 (int)((1-AVG(in.ArrDel15))*100) AS performance_index;
 
sorted_airlines = ORDER average_ontime BY performance_index DESC; 
top_ten_airlines = LIMIT sorted_airlines 10;
-- X = FOREACH top_ten_airlines GENERATE FLATTEN(top_ten_airlines);

DUMP top_ten_airlines;
--DUMP X;

