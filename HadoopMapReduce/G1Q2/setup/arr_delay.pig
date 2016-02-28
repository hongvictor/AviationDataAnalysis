--------------------------------------------------------------
-- NAME:
--     arr_delay.pig 
-- INPUT 
--     $PIG_IN_DIR
-- OUTPUT
--     $PIG_IN_DIR
-- FILEDS
--     AirlineID 
--     ArrDel15
-- REFERENCE
--     http://www.transtats.bts.gov/Fields.asp?Table_ID=236
-- DATA SOURCE
--     /data/aviation/airline_ontime
--------------------------------------------------------------
A = LOAD '$PIG_IN_DIR' USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS 
(a1, a2, a3, a4, a5,
 a6, a7, a8:chararray, a9, a10,
 a11, a12, a13, a14, a15,
 a16, a17, a18, a19, a20,
 a21, a22, a23, a24, a25,
 a26, a27, a28, a29, a30,
 a31, a32, a33, a34, a35,
 a36, a37, a38, a39:int, a40
);
	
-- a8	AirlineID
-- a39	ArrDel15

B = foreach A generate a8, a39;  -- extract  

store B into '$PIG_OUT_DIR';  -- write the results to a folder

