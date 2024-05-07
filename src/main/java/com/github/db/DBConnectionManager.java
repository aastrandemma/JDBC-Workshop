package com.github.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnectionManager {
    private static final String JDBC_USER = "root";
    private static final String JDBC_PWD = "1234";
    private static final String JDBC_URL = "jdbc:mysql://localhost:3306/world";

    public static Connection getConnection() {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PWD);
        } catch (SQLException e) {
            System.out.println("SQL Exception: " + e.getMessage());
        }
        return connection;
    }
}