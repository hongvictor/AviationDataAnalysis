--------------------------------------------------------------
-- NAME:
--     topairports.pig 
--
-- INPUT 
--     $PIG_IN_DIR
-- OUTPUT
--     $PIG_OUT_DIR
-- FIELDS
--     AirlineID 
--     Origin
--     Dest
--     DepDel15
--     ArrDel15
-- REFERENCE
--     http://www.transtats.bts.gov/Fields.asp?Table_ID=236
--------------------------------------------------------------
-- raw_in  = LOAD '$PIG_IN_DIR' AS 

in  = LOAD '$PIG_IN_DIR' AS 
      ( AirlineID:chararray,
        Origin:chararray,
        Dest:chararray,
        DepDel15:int,
        ArrDel15:int
      );

-- test only	
-- in = FILTER raw_in BY ( Origin == 'CMI' OR Origin == 'BWI' OR Origin == 'MIA' );

group_by_origin_dest = GROUP in BY (Origin,Dest);

average_ontime = FOREACH group_by_origin_dest 
                 GENERATE FLATTEN(group) AS (Origin, Dest), 
                          (int)(100 * (1-AVG(in.DepDel15))) AS performance_index;

group_by_origin = GROUP average_ontime BY Origin; 
 
top_ten_airports = FOREACH group_by_origin {
   sorted_airports = ORDER average_ontime BY performance_index DESC;
   top_airports = LIMIT sorted_airports 10;
   GENERATE FLATTEN(top_airports);
}

X = FOREACH top_ten_airports GENERATE TOTUPLE( TOTUPLE( 'origin',$0), TOTUPLE('dest', $1 )), TOTUPLE($2);

-- DUMP X;

STORE X into 'cql://temp/t1g2q2?output_query=update%20t1g2q2%20set%20ontimeratio%20%3D%3F' USING CqlStorage();  -- write the results to a folder

