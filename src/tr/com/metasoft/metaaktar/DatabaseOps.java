/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tr.com.metasoft.metaaktar;

import java.sql.*;

/**
 * Created by Cagdas Tunca on 10.04.2017.
 */
public class DatabaseOps {

    private Connection connection;
    private String ip;
    private String port;
    private String dbName;
    private String dbUser;
    private String dbPass;

    public DatabaseOps(String ip, String port, String dbName, String dbUser, String dbPass) {
        try {
            Class.forName("net.sourceforge.jtds.jdbc.Driver");
            Connection conn = DriverManager.getConnection("jdbc:jtds:sqlserver://"
                    + ip + ":" + port + "/" + dbName, dbUser, dbPass);

            System.out.println("connected");
            this.connection = conn;

            //loggingde toString icin
            this.ip = ip;
            this.port = port;
            this.dbName = dbName;
            this.dbUser = dbUser;
            this.dbPass = dbPass;

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void insertAttributeHexBlob(long pk, String hex) throws SQLException {
        String query = "SET IDENTITY_INSERT dicomattrs on "
                + " "
                + "insert into dicomattrs (pk,attrs) values (" + pk + "," + hex + ") "
                + " "
                + "SET IDENTITY_INSERT dicomattrs off "
                + "";

        Statement state = connection.createStatement();
        state.execute(query);
    }

    //region PATIENT ATTRIBUTES BLOB ISLEMLERI
    public int getPatientTableCount() throws SQLException {
        String query = "SELECT    COUNT(*) as count\n"
                + "FROM            patient INNER JOIN\n"
                + "                         person_name ON patient.pat_name_fk = person_name.pk INNER JOIN\n"
                + "                         patient_id ON patient.patient_id_fk = patient_id.pk";
        Statement state = connection.createStatement();
        ResultSet rs = state.executeQuery(query);
        rs.next();
        return rs.getInt("count");
    }

    public ResultSet getPatientTable(long start, long stop) throws SQLException {
        String query = "SELECT  *\n"
                + "FROM    ( \n"
                + "\t\t\tSELECT    ROW_NUMBER() OVER ( ORDER BY patient.pk ) AS RowNum, \n"
                + "\t\t\t\t\tpatient.pk, person_name.family_name, person_name.given_name, patient_id.pat_id, patient.pat_birthdate, patient.pat_sex, patient.pat_custom1\n"
                + "\t\t\tFROM            patient INNER JOIN\n"
                + "                         person_name ON patient.pat_name_fk = person_name.pk INNER JOIN\n"
                + "                         patient_id ON patient.patient_id_fk = patient_id.pk) as RowConstrainedResult\n"
                + "WHERE   RowNum >= " + start + "\n"
                + "    AND RowNum < " + stop + "\n"
                + "ORDER BY RowNum";

        Statement state = connection.createStatement();
        return state.executeQuery(query);
    }

    public void updateDicomAttrsFkForPatient(long pk, long dicomattrs_fk) throws SQLException {
        String query = "UPDATE       patient\n"
                + "SET          dicomattrs_fk = " + dicomattrs_fk + "\n"
                + "WHERE        (pk = " + pk + ")\t\t";

        Statement state = connection.createStatement();
        state.execute(query);
    }

    //endregion
    //region STUDY ATTRIBUTES BLOB ISLEMLERI
    public int getStudyTableCount() throws SQLException {
        String query = "SELECT    COUNT(*) as count\n"
                + "FROM           study";
        Statement state = connection.createStatement();
        ResultSet rs = state.executeQuery(query);
        rs.next();
        return rs.getInt("count");
    }

    public ResultSet getStudyTable(long start, long stop) throws SQLException {
        String query = "SELECT  *\n"
                + "FROM    ( \n"
                + "\t\t\tSELECT    ROW_NUMBER() OVER ( ORDER BY pk ) AS RowNum, \n"
                + "\t\t\t\t\tpk, study_date, study_time, accession_no, study_iuid, study_id\n"
                + "FROM            study) as RowConstrainedResult\n"
                + "WHERE   RowNum >= " + start + "\n"
                + "    AND RowNum < " + stop + "\n"
                + "ORDER BY RowNum";

        Statement state = connection.createStatement();
        return state.executeQuery(query);
    }

    public void updateDicomAttrsFkForStudy(long pk, long dicomattrs_fk) throws SQLException {
        String query = "UPDATE       study\n"
                + "SET          dicomattrs_fk = " + dicomattrs_fk + "\n"
                + "WHERE        (pk = " + pk + ")\t\t";

        Statement state = connection.createStatement();
        state.execute(query);
    }

    //endregion
    //region SERIES ATTRIBUTES BLOB ISLEMLERI
    public int getSeriesTableCount() throws SQLException {
        String query = "SELECT    COUNT(*) as count\n"
                + "FROM           series";
        Statement state = connection.createStatement();
        ResultSet rs = state.executeQuery(query);
        rs.next();
        return rs.getInt("count");
    }

    public ResultSet getSeriesTable(long start, long stop) throws SQLException {
        String query = "SELECT  *\n"
                + "FROM    ( \n"
                + "\t\t\tSELECT    ROW_NUMBER() OVER ( ORDER BY pk ) AS RowNum, \n"
                + "\t\t\t\t\tpk, modality, series_iuid, series_no\n"
                + "FROM            series) as RowConstrainedResult\n"
                + "WHERE   RowNum >= " + start + "\n"
                + "    AND RowNum < " + stop + "\n"
                + "ORDER BY RowNum";

        Statement state = connection.createStatement();
        return state.executeQuery(query);
    }

    public void updateDicomAttrsFkForSeries(long pk, long dicomattrs_fk) throws SQLException {
        String query = "UPDATE       series\n"
                + "SET          dicomattrs_fk = " + dicomattrs_fk + "\n"
                + "WHERE        (pk = " + pk + ")\t\t";

        Statement state = connection.createStatement();
        state.execute(query);
    }

    //endregion
    //region INSTANCE ATTRIBUTES BLOB ISLEMLERI
    public int getInstanceTableCount() throws SQLException {
        String query = "SELECT    COUNT(*) as count\n"
                + "FROM           instance INNER JOIN\n"
                + "                         location ON instance.pk = location.instance_fk ";
        //+ "                         INNER JOIN\n aktarim ON location.pk = aktarim.location_fk";
        Statement state = connection.createStatement();
        ResultSet rs = state.executeQuery(query);
        rs.next();
        return rs.getInt("count");
    }

    public ResultSet getInstanceTable(long start, long stop) throws SQLException {
        String query = "SELECT  *\n"
                + "FROM    ( \n"
                + "\t\t\tSELECT    ROW_NUMBER() OVER ( ORDER BY instance.pk ) AS RowNum, \n"
                + "\t\t\t\t\tinstance.pk, instance.sop_cuid, instance.sop_iuid, instance.inst_no, instance.num_frames,  location.storage_path\n\n"
                +//aktarim.Rows, aktarim.Columns,
                "FROM            instance INNER JOIN\n"
                + "                         location ON instance.pk = location.instance_fk"
                + "                         ) as RowConstrainedResult\n"
                +//INNER JOIN aktarim ON location.pk = aktarim.location_fk
                "WHERE   RowNum >=" + start + "\n"
                + "    AND RowNum < " + stop + "\n"
                + "ORDER BY RowNum";

        Statement state = connection.createStatement();
        return state.executeQuery(query);
    }

    public void updateDicomAttrsFkForInstance(long pk, long dicomattrs_fk) throws SQLException {
        String query = "UPDATE       instance\n"
                + "SET          dicomattrs_fk = " + dicomattrs_fk + "\n"
                + "WHERE        (pk = " + pk + ")\t\t";

        Statement state = connection.createStatement();
        state.execute(query);
    }

    //endregion
    //region DICOMATTRS ISLEMLERI
    public int getDicomattrsAvailablePkStart() throws SQLException {
        String query = "SELECT *\n"
                + "  FROM  dicomattrs order by pk DESC";//select max(pk) from dicomattrs
        Statement state = connection.createStatement();
        ResultSet rs = state.executeQuery(query);
        if (rs.next()) {
            return (rs.getInt("pk") + 1);
        }
        return 0;
    }

    //endregion
    //region AKTARIM TABLE ISLEMLERI
    public void updateAktarimRow(String sop_iuid, String retrieve_aets, int bitsAllocated, int rows, int columns) throws SQLException {
        String sql = "UPDATE       aktarim\n"
                + "SET\n"
                + "Rows =" + rows + ", Columns =" + columns + ",\n"
                + "BitsAllocated =" + bitsAllocated + "\n"
                + "FROM            aktarim INNER JOIN\n"
                + "                         location ON aktarim.location_fk = location.pk INNER JOIN\n"
                + "                         instance ON location.instance_fk = instance.pk\n"
                + " where sop_iuid='" + sop_iuid + "' and retrieve_aets='" + retrieve_aets + "'";

        Statement state = connection.createStatement();
        state.execute(sql);
    }
    //endregion

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
    
    public static void main(String[] args) {
        String query = "UPDATE       instance\n"
                + "SET          dicomattrs_fk = " + 1 + "\n"
                + "WHERE        (pk = " + 12 + ")\t\t";
        
        System.out.println(query);
    }
}
