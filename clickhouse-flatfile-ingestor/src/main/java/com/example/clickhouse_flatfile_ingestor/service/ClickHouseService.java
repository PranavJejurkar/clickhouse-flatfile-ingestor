package com.example.clickhouse_flatfile_ingestor.service;



import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.CSVParser;
import org.springframework.stereotype.Service;

import java.io.*;
import java.sql.*;
import java.util.*;

@Service
public class ClickHouseService {

    public Connection connect(String host, String port, String database, String user, String jwtToken) throws SQLException {
        String url = "jdbc:clickhouse://" + host + ":" + port + "/" + database;
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", jwtToken);
        return DriverManager.getConnection(url, props);
    }

    public int exportToCSV(String host, String port, String db, String user, String token,
                           String table, String filepath) throws Exception {
        try (Connection conn = connect(host, port, db, user, token);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + table);
             FileWriter out = new FileWriter(filepath);
             CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT.withHeader(rs))) {

            ResultSetMetaData meta = rs.getMetaData();
            int columns = meta.getColumnCount();
            int rowCount = 0;

            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= columns; i++) {
                    row.add(rs.getString(i));
                }
                printer.printRecord(row);
                rowCount++;
            }
            return rowCount;
        }
    }

    public int importFromCSV(String host, String port, String db, String user, String token,
                             String table, String filepath) throws Exception {
        try (Connection conn = connect(host, port, db, user, token);
             Reader in = new FileReader(filepath);
             CSVParser parser = new CSVParser(in, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {

            List<String> headers = parser.getHeaderNames();
            String columns = String.join(",", headers);

            String placeholders = String.join(",", Collections.nCopies(headers.size(), "?"));
            String sql = "INSERT INTO " + table + " (" + columns + ") VALUES (" + placeholders + ")";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                int rowCount = 0;
                for (CSVRecord record : parser) {
                    for (int i = 0; i < headers.size(); i++) {
                        pstmt.setString(i + 1, record.get(i));
                    }
                    pstmt.addBatch();
                    rowCount++;
                }
                pstmt.executeBatch();
                return rowCount;
            }
        }
    }
}
