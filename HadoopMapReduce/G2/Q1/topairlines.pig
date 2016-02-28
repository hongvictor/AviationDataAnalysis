--------------------------------------------------------------
-- NAME:
--     topairlines.pig 
--
-- INPUT PARAMS
--     $PIG_IN_DIR
-- OUTPUT
--     $PIG_OUT_DIR
-- FIELDS
--     airlineID 
--     Origin
--     Dest
--     DepDel15
--     ArrDel15
-- REFERENCE
--     http://www.transtats.bts.gov/Fields.asp?Table_ID=236
--------------------------------------------------------------

in  = LOAD '$PIG_IN_DIR' AS 
          ( AirlineID:chararray,
            Origin:chararray,
            Dest:chararray,
            DepDel15:int,
            ArrDel15:int
          );
	
group_by_origin_airline = GROUP in BY (Origin,AirlineID);

average_ontime = FOREACH group_by_origin_airline 
                 GENERATE FLATTEN(group) AS (Origin, AirlineID), 
                          (int)((1-AVG(in.DepDel15))*100) AS performance_index;

group_by_origin = GROUP average_ontime BY Origin; 
 
top_ten_airlines = FOREACH group_by_origin {
   sorted_airlines = ORDER average_ontime BY performance_index DESC;
   top_airlines = LIMIT sorted_airlines 10;
   GENERATE FLATTEN(top_airlines);
}

X = FOREACH top_ten_airlines GENERATE TOTUPLE( TOTUPLE( 'origin',$0), TOTUPLE('airline', $1 )), TOTUPLE($2);

STORE X into 'cql://temp/t1g2q1?output_query=update%20t1g2q1%20set%20ontimeratio%20%3D%3F' USING CqlStorage();  

