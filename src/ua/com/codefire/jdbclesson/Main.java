/*
 * CodeFireUA public license.
 */
package ua.com.codefire.jdbclesson;

import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author CodeFireUA <edu@codefire.com.ua>
 */
public class Main {

    /**
     * @param args the command line arguments
     * @throws java.lang.ClassNotFoundException
     */
    public static void main(String[] args) throws ClassNotFoundException, IOException {

        Class.forName(com.mysql.jdbc.Driver.class.getName());

        List<String> tableList = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/test", "student", "12345")) {

            ResultSet rsTables = conn.createStatement().executeQuery("SHOW TABLES");

            System.out.println("Available tables:");
            int i = 1;
            while (rsTables.next()) {
                String tableName = rsTables.getString(1);
                tableList.add(tableName);

                System.out.printf(" %2d. %s\n", i++, tableName);
            }
        } catch (SQLException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }

        int tableNumber;

        Scanner scanner = new Scanner(System.in);

        while (true) {
            System.out.println("Choose table:");

            try {
                tableNumber = Integer.parseInt(scanner.nextLine());
                break;
            } catch (NumberFormatException ex) {
                System.out.println("Wrong input.");
            }
        }

        try (Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/test", "student", "12345")) {
            String selectedTableName = tableList.get(tableNumber - 1);
            String selectQuery = String.format("SELECT * FROM `%s`", selectedTableName);

            ResultSet rs = conn.createStatement().executeQuery(selectQuery);
            ResultSetMetaData meta = rs.getMetaData();
//
            int columnCount = meta.getColumnCount();
            Vector<String> columnNames = new Vector<>();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = meta.getColumnName(i);
                columnNames.add(columnName);
            }

            //
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rsPK = metaData.getPrimaryKeys(null, "root", selectedTableName);
            ResultSetMetaData rsPKMeta = rs.getMetaData();
            DateFormat df = new SimpleDateFormat("yyyyMMddHHmm");
            String time = df.format(new Date(System.currentTimeMillis()));
            System.out.println("Now: " + time);

            String nameFile = selectedTableName + time + ".sql";
            try {

                FileOutputStream fos = new FileOutputStream(nameFile, true);
                String line = String.format("CREATE TABLE `%s` (\n", selectedTableName);
                // String line = "CREATE TABLE `" + selectedTableName + "` (\n";
                byte[] bytes = line.getBytes();
                fos.write(bytes);
                fos.close();

            } catch (IOException ex) {
                ex.printStackTrace();
            }

            System.out.println("");

            System.out.print("|");
            try (FileOutputStream fos = new FileOutputStream(nameFile, true)) {

                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    String columnName = meta.getColumnName(i);
//                    String columnType = meta.getColumnTypeName(i);

                    int columnSize = meta.getColumnDisplaySize(i);

                    switch (meta.getColumnType(1)) {
                        case Types.DATE: {
                            String line = String.format("  `%s` %s", columnName, Types.DATE);
                            byte[] bytes = line.getBytes();
                            fos.write(bytes);
                        }
                        break;
                        default: {
                            String line = String.format("  `%s` %s(%s)", columnName, meta.getColumnTypeName(i), columnSize);
                            byte[] bytes = line.getBytes();
                            fos.write(bytes);
                        }
                        break;
                    }

                    if (i < meta.getColumnCount()) {
                        byte[] bytes = ",\n".getBytes();
                        fos.write(bytes);
                    }
                }
                byte[] bytes = "\n);\n".getBytes();
                fos.write(bytes);

                StringBuilder sbQueries = new StringBuilder(String.format("INSERT INTO `%s` VALUES\n", selectedTableName));

                while (rs.next()) {
                    sbQueries.append("(");

                    for (int i = 1; i <= columnCount; i++) {
                        Object value = rs.getObject(i);

                        if (value != null) {
                            sbQueries.append("'").append(rs.getObject(i)).append("'");
                        } else {
                            sbQueries.append("NULL");
                        }

                        if (i < columnCount) {
                            sbQueries.append(", ");
                        }
                    }
                    sbQueries.append(")");

                    sbQueries.append(rs.isLast() ? ";\n" : ",\n");
                }

                fos.write(sbQueries.toString().getBytes());
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            System.out.println();

        } catch (SQLException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
