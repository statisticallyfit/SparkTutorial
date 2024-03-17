//package com.sparkstreaming.sparkstreamingjoins.pointInTimeJoins.examplesDuckDB
//
//import io.deephaven.qst.table.InputTable
//import io.deephaven.qst.table.NewTable
//
//import io.deephaven.base.
//
//import org.duckdb.DuckDBConnection
//
//import java.sql.Statement
//
//
///**
// *
// */
//class AboutEventTable {
//
//
//	trades = newTable(
//		stringCol("Ticker", "AAPL", "AAPL", "AAPL", "IBM", "IBM"),
//		instantCol("Timestamp", parseInstant("2021-04-05T09:10:00 ET"), parseInstant("2021-04-05T09:31:00 ET"), parseInstant("2021-04-05T16:00:00 ET"), parseInstant("2021-04-05T16:00:00 ET"), parseInstant("2021-04-05T16:30:00 ET")),
//		doubleCol("Price", 2.5, 3.7, 3.0, 100.50, 110),
//		intCol("Size", 52, 14, 73, 11, 6)
//	)
//
//	quotes = newTable(
//		stringCol("Ticker", "AAPL", "AAPL", "IBM", "IBM", "IBM"),
//		instantCol("Timestamp", parseInstant("2021-04-05T09:11:00 ET"), parseInstant("2021-04-05T09:30:00 ET"), parseInstant("2021-04-05T16:00:00 ET"), parseInstant("2021-04-05T16:30:00 ET"), parseInstant("2021-04-05T17:00:00 ET")),
//		doubleCol("Bid", 2.5, 3.4, 97, 102, 108),
//		intCol("BidSize", 10, 20, 5, 13, 23),
//		doubleCol("Ask", 2.5, 3.4, 105, 110, 111),
//		intCol("AskSize", 83, 33, 47, 15, 5),
//	)
//
//	result = trades.aj(quotes, "Ticker, Timestamp")
//
//
//
//
//	// TODO need this?
//	//  Class.forName("org.duckdb.DuckDBDriver")
//
//	import java.sql.Connection;
//	import java.sql.DriverManager;
//
//	val conn: Connection = DriverManager.getConnection("jdbc:duckdb:");
//	val duckdbconn: DuckDBConnection = conn.asInstanceOf[DuckDBConnection]
//	val stmt: Statement = conn.createStatement // TODO use the duckdb conn or not?
//
//
//	stmt.execute("CREATE TABLE items (item VARCHAR, value DECIMAL(10, 2), count INTEGER)")
//	// insert two items into the table// insert two items into the table
//
//	stmt.execute("INSERT INTO items VALUES ('jeans', 20.0, 1), ('hammer', 42.2, 2)")
//
//	try {
//		val rs = stmt.executeQuery("SELECT * FROM items")
//		while (rs.next) {
//			System.out.println(rs.getString(1))
//			System.out.println(rs.getInt(3))
//		}
//		//finally if (rs != null) rs.close()
//	}
//	// jeans
//	/*stmt.execute("CREATE TABLE test (x INT, y INT, z INT)")
//
//	stmt.addBatch("INSERT INTO test (x, y, z) VALUES (1, 2, 3);")
//	stmt.addBatch("INSERT INTO test (x, y, z) VALUES (4, 5, 6);")
//
//	stmt.executeBatch*/
//}
