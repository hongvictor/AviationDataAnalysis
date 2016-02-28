--------------------------------------------------------------
-- NAME:
--     ontimeperf.pig 
--
-- INPUT 
--     $PIG_IN_DIR
-- OUTPUT
--     $PIG_OUT_DIR
-- FIELDS
-- a7   UniqueCarrier
-- a8   AirlineID
-- a12  Origin
-- a18  Dest
-- a28  DepDel15
-- a39  ArrDel15
-- a42  Cancelled
-- a44  Diverted
--
-- REFERENCE
--     http://www.transtats.bts.gov/Fields.asp?Table_ID=236
-- DATA SOURCE
--     /data/aviation/airline_ontime
--------------------------------------------------------------
A = LOAD '$PIG_IN_DIR' USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS 
(a1, a2, a3, a4, a5,
 a6, a7:chararray, a8:chararray, a9, a10,
 a11, a12:chararray, a13, a14, a15,
 a16, a17, a18:chararray, a19, a20,
 a21, a22, a23, a24, a25,
 a26, a27, a28:int, a29, a30,
 a31, a32, a33, a34, a35,
 a36, a37, a38, a39:int, a40,
 a41, a42:int, a43, a44:int, a45
);

-- a7   UniqueCarrier
-- a8   AirlineID
-- a12  Origin
-- a18  Dest
-- a28  DepDel15
-- a39  ArrDel15
-- a42  Cancelled
-- a44  Diverted
	
B = FILTER A by ( a28 is not null ) and ( a39 is not null ) and ( a42 == 0 ) and ( a44 == 0 ); 
C = FOREACH B GENERATE a7, a8, a12, a18, a28, a39;  -- extract  

STORE C into '$PIG_OUT_DIR';  -- write the results to a folder

