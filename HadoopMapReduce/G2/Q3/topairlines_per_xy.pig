--------------------------------------------------------------
-- NAME:
--     topairlines_per_xy.pig 
--
-- INPUT 
--     $PIG_IN_DIR
-- OUTPUT
--     cql://temp/t1g2q3
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
	
-- in = FILTER raw_in BY ( (Origin == 'CMI' AND Dest=='ORD') OR (Origin == 'IND' AND Dest=='CMH') OR (Origin == 'DFW' AND Dest=='IAH') OR (Origin == 'LAX' AND Dest=='SFO') OR (Origin == 'JFK' AND Dest=='LAX') OR (Origin == 'ATL' AND Dest=='PHX') );

group_by_origin_dest_airline = GROUP in BY (Origin, Dest, AirlineID);

average_ontime = FOREACH group_by_origin_dest_airline
               GENERATE FLATTEN(group) AS (Origin, Dest, AirlineID),
               (int) (100 * (1-AVG(in.ArrDel15))) AS performance_index;

group_by_origin_dest = GROUP average_ontime BY (Origin, Dest);

top_ten_airlines = FOREACH group_by_origin_dest {
   sorted_airlines = ORDER average_ontime BY performance_index DESC;
   top_airlines = LIMIT sorted_airlines 10;
   GENERATE FLATTEN(top_airlines);
}

X = FOREACH top_ten_airlines GENERATE TOTUPLE( TOTUPLE('origin',$0), TOTUPLE('dest', $1 ),TOTUPLE('airline', $2 )), TOTUPLE($3);

-- dump X;

STORE X into 'cql://temp/t1g2q3?output_query=update%20t1g2q3%20set%20ontimeratio%20%3D%3F' USING CqlStorage();  

