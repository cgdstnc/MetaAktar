/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tr.com.metasoft.metaaktar.System.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import tr.com.metasoft.metaaktar.System.publicEnum.KayitTipi;
import static tr.com.metasoft.metaaktar.System.publicEnum.KayitTipi.HEPSI;

/**
 *
 * @author Administrator
 */
public class DatabaseOperations {

    private static Connection connection;
    private static String ip;
    private static String port;
    private static String dbName;
    private static String dbUser;
    private static String dbPass;

    public static void connect(String s_ip, String s_port, String s_dbName, String s_dbUser, String s_dbPass) throws Exception {

        Class.forName("net.sourceforge.jtds.jdbc.Driver");
        Connection conn = DriverManager.getConnection("jdbc:jtds:sqlserver://"
                + s_ip + ":" + s_port + "/" + s_dbName, s_dbUser, s_dbPass);

        System.out.println(s_dbName + " veri tabanına bağlandı.");
        connection = conn;

        //loggingde toString icin
        ip = s_ip;
        port = s_port;
        dbName = s_dbName;
        dbUser = s_dbUser;
        dbPass = s_dbPass;

    }

    //**************************
    //PATIENT*******************
    //**************************
    public static int getPatientCount(KayitTipi tip) throws SQLException {
        String query = "";
        switch (tip) {
            case HEPSI:
                query = Queries.getPatientToplamCountQuery();
                break;
            case AKTARILMIS:
                query = Queries.getPatientAktarilmisCountQuery();
                break;
            case AKTARILMAMIS:
                query = Queries.getPatientAktarilmamisCountQuery();
                break;
            default:
                query = Queries.getPatientToplamCountQuery();
                break;
        }
        Statement state = connection.createStatement();
        ResultSet rs = state.executeQuery(query);
        rs.next();
        return rs.getInt("count");
    }

    public static ResultSet getPatientTable(KayitTipi tip, long start, long stop) throws SQLException {
        String query = "";
        switch (tip) {
            case HEPSI:
                query = Queries.getPatientTumTabloQuery(start, stop);
                break;
            case AKTARILMIS:
                query = Queries.getPatientAktarilmisTabloQuery(start, stop);
                break;
            case AKTARILMAMIS:
                query = Queries.getPatientAktarilmamisTabloQuery(start, stop);
                break;
            default:
                query = Queries.getPatientTumTabloQuery(start, stop);
                break;
        }

        Statement state = connection.createStatement();
        return state.executeQuery(query);
    }

    public static boolean updateDicomAttrsFkForPatient(long pk, long dicomattrs_fk) throws SQLException {
        String query = Queries.getPatientDicomAttrsUpdateQuery(dicomattrs_fk, pk);
        Statement state = connection.createStatement();
        return state.execute(query);
    }

    //**************************
    //STUDY*********************
    //**************************
    public static int getStudyCount(KayitTipi tip) throws SQLException {
        String query = "";
        switch (tip) {
            case HEPSI:
                query = Queries.getStudyToplamCountQuery();
                break;
            case AKTARILMIS:
                query = Queries.getStudyAktarilmisCountQuery();
                break;
            case AKTARILMAMIS:
                query = Queries.getStudyAktarilmamisCountQuery();
                break;
            default:
                query = Queries.getStudyToplamCountQuery();
                break;
        }
        Statement state = connection.createStatement();
        ResultSet rs = state.executeQuery(query);
        rs.next();
        return rs.getInt("count");
    }

    public static ResultSet getStudyTable(KayitTipi tip, long start, long stop) throws SQLException {
        String query = "";
        switch (tip) {
            case HEPSI:
                query = Queries.getStudyTumTabloQuery(start, stop);
                break;
            case AKTARILMIS:
                query = Queries.getStudyAktarilmisTabloQuery(start, stop);
                break;
            case AKTARILMAMIS:
                query = Queries.getStudyAktarilmamisTabloQuery(start, stop);
                break;
            default:
                query = Queries.getStudyTumTabloQuery(start, stop);
                break;
        }

        Statement state = connection.createStatement();
        return state.executeQuery(query);
    }

    public static boolean updateDicomAttrsFkForStudy(long pk, long dicomattrs_fk) throws SQLException {
        String query = Queries.getStudyDicomAttrsUpdateQuery(dicomattrs_fk, pk);
        Statement state = connection.createStatement();
        return state.execute(query);
    }

    //**************************
    //SERIES********************
    //**************************
    public static int getSeriesCount(KayitTipi tip) throws SQLException {
        String query = "";
        switch (tip) {
            case HEPSI:
                query = Queries.getSeriesToplamCountQuery();
                break;
            case AKTARILMIS:
                query = Queries.getSeriesAktarilmisCountQuery();
                break;
            case AKTARILMAMIS:
                query = Queries.getSeriesAktarilmamisCountQuery();
                break;
            default:
                query = Queries.getSeriesToplamCountQuery();
                break;
        }
        Statement state = connection.createStatement();
        ResultSet rs = state.executeQuery(query);
        rs.next();
        return rs.getInt("count");
    }

    public static ResultSet getSeriesTable(KayitTipi tip, long start, long stop) throws SQLException {
        String query = "";
        switch (tip) {
            case HEPSI:
                query = Queries.getSeriesTumTabloQuery(start, stop);
                break;
            case AKTARILMIS:
                query = Queries.getSeriesAktarilmisTabloQuery(start, stop);
                break;
            case AKTARILMAMIS:
                query = Queries.getSeriesAktarilmamisTabloQuery(start, stop);
                break;
            default:
                query = Queries.getSeriesTumTabloQuery(start, stop);
                break;
        }

        Statement state = connection.createStatement();
        return state.executeQuery(query);
    }

    public static boolean updateDicomAttrsFkForSeries(long pk, long dicomattrs_fk) throws SQLException {
        String query = Queries.getSeriesDicomAttrsUpdateQuery(dicomattrs_fk, pk);
        Statement state = connection.createStatement();
        return state.execute(query);
    }

    //**************************
    //INSTANCE******************
    //**************************
    public static int getInstanceCount(KayitTipi tip) throws SQLException {
        String query = "";
        switch (tip) {
            case HEPSI:
                query = Queries.getInstanceToplamCountQuery();
                break;
            case AKTARILMIS:
                query = Queries.getInstanceAktarilmisCountQuery();
                break;
            case AKTARILMAMIS:
                query = Queries.getInstanceAktarilmamisCountQuery();
                break;
            default:
                query = Queries.getInstanceToplamCountQuery();
                break;
        }
        Statement state = connection.createStatement();
        ResultSet rs = state.executeQuery(query);
        rs.next();
        return rs.getInt("count");
    }

    public static ResultSet getInstanceTable(KayitTipi tip, long start, long stop) throws SQLException {
        String query = "";
        switch (tip) {
            case HEPSI:
                query = Queries.getInstanceTumTabloQuery(start, stop);
                break;
            case AKTARILMIS:
                query = Queries.getInstanceAktarilmisTabloQuery(start, stop);
                break;
            case AKTARILMAMIS:
                query = Queries.getInstanceAktarilmamisTabloQuery(start, stop);
                break;
            default:
                query = Queries.getInstanceTumTabloQuery(start, stop);
                break;
        }

        Statement state = connection.createStatement();
        return state.executeQuery(query);
    }

    public static boolean updateDicomAttrsFkForInstance(long pk, long dicomattrs_fk) throws SQLException {
        String query = Queries.getInstanceDicomAttrsUpdateQuery(dicomattrs_fk, pk);
        Statement state = connection.createStatement();
        return state.execute(query);
    }

    //**************************
    //DICOMATTRS****************
    //**************************
    public static void insertAttributeHexBlob(long pk, String hex) throws SQLException {
        String query = Queries.getDicomAttrsInsertAttributeHexBlobQuery(pk, hex);
        Statement state = connection.createStatement();
        state.execute(query);
    }

    public static int getDicomAttrsMaxPk() throws SQLException {
        Statement state = connection.createStatement();
        ResultSet rs = state.executeQuery(Queries.getDicomAttrsMaxPkQuery());
        rs.next();
        return rs.getInt("pk");
    }

    @Override
    public String toString() {
        return "DatabaseOps{"
                + ", ip='" + ip + '\''
                + ", port='" + port + '\''
                + ", dbName='" + dbName + '\''
                + ", dbUser='" + dbUser + '\''
                + ", dbPass='" + dbPass + '\''
                + "connection=" + connection
                + '}';
    }
}
