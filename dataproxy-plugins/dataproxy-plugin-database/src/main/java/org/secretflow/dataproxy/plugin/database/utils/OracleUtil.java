package org.secretflow.dataproxy.plugin.database.utils;

import org.secretflow.dataproxy.plugin.database.config.DatabaseConnectConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class OracleUtil {
    public static Connection initOracle(DatabaseConnectConfig config) throws SQLException {
        String endpoint = config.endpoint();
        String ip;
        int port = 1521; // 默认端口

        if (endpoint.contains(":")) {
            String[] parts = endpoint.split(":");
            ip = parts[0];
            if (parts.length > 1 && !parts[1].isEmpty()) {
                port = Integer.parseInt(parts[1]);
            }
        } else {
            ip = endpoint;
        }
        Connection conn;
        try{

            if(!config.username().isEmpty() && !config.password().isEmpty()) {
                conn = DriverManager.getConnection(String.format("jdbc:oracle:thin:@%s:%d:%s", ip, port, config.database()), config.username(), config.password());
            } else {
                conn = DriverManager.getConnection(String.format("jdbc:oracle:thin:@%s:%d:%s", ip, port, config.database()));
            }
        } catch (Exception e) {
            System.out.println("database init error");
            throw new RuntimeException(e);
        }

        return conn;

    }
}
