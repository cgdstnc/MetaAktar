/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tr.com.metasoft.metaaktar.System.database;

/**
 *
 * @author Administrator
 */
public class Queries {

    //**************************
    //PATIENT*******************
    //**************************
    public static String getPatientToplamCountQuery() {
        return "SELECT    COUNT(*) as count\n"
                + "FROM            patient INNER JOIN\n"
                + "                         person_name ON patient.pat_name_fk = person_name.pk INNER JOIN\n"
                + "                         patient_id ON patient.patient_id_fk = patient_id.pk";
    }

    public static String getPatientAktarilmamisCountQuery() {
        return "SELECT    COUNT(*) as count\n"
                + "FROM            patient INNER JOIN\n"
                + "                         person_name ON patient.pat_name_fk = person_name.pk INNER JOIN\n"
                + "                         patient_id ON patient.patient_id_fk = patient_id.pk "
                + " WHERE dicomattrs_fk = 0";
    }

    public static String getPatientAktarilmisCountQuery() {
        return "SELECT    COUNT(*) as count\n"
                + "FROM            patient INNER JOIN\n"
                + "                         person_name ON patient.pat_name_fk = person_name.pk INNER JOIN\n"
                + "                         patient_id ON patient.patient_id_fk = patient_id.pk "
                + " WHERE dicomattrs_fk!=0";
    }

    public static String getPatientTumTabloQuery(long pagerStart, long pagerStop) {
        return "SELECT  *\n"
                + "FROM    ( \n"
                + "			SELECT    ROW_NUMBER() OVER ( ORDER BY patient.pk ) AS RowNum, \n"
                + "					patient.pk, person_name.family_name, person_name.given_name, patient_id.pat_id, patient.pat_birthdate, patient.pat_sex, patient.pat_custom1\n"
                + "			FROM            patient INNER JOIN\n"
                + "                         person_name ON patient.pat_name_fk = person_name.pk INNER JOIN\n"
                + "                         patient_id ON patient.patient_id_fk = patient_id.pk) as RowConstrainedResult\n"
                + " WHERE   RowNum >= " + pagerStart + "\n"
                + "    AND RowNum < " + pagerStop + "\n"
                + "ORDER BY RowNum";
    }

    public static String getPatientAktarilmamisTabloQuery(long pagerStart, long pagerStop) {
        return "SELECT  *\n"
                + "FROM    ( \n"
                + "			SELECT    ROW_NUMBER() OVER ( ORDER BY patient.pk ) AS RowNum, \n"
                + "					patient.pk, person_name.family_name, person_name.given_name, patient_id.pat_id, patient.pat_birthdate, patient.pat_sex, patient.pat_custom1\n"
                + "			FROM            patient INNER JOIN\n"
                + "                         person_name ON patient.pat_name_fk = person_name.pk INNER JOIN\n"
                + "                         patient_id ON patient.patient_id_fk = patient_id.pk where dicomattrs_fk=0) as RowConstrainedResult\n"
                + " WHERE   RowNum >= " + pagerStart + "\n"
                + "    AND RowNum < " + pagerStop + "\n"
                + "ORDER BY RowNum";
    }

    public static String getPatientAktarilmisTabloQuery(long pagerStart, long pagerStop) {
        return "SELECT  *\n"
                + "FROM    ( \n"
                + "			SELECT    ROW_NUMBER() OVER ( ORDER BY patient.pk ) AS RowNum, \n"
                + "					patient.pk, person_name.family_name, person_name.given_name, patient_id.pat_id, patient.pat_birthdate, patient.pat_sex, patient.pat_custom1\n"
                + "			FROM            patient INNER JOIN\n"
                + "                         person_name ON patient.pat_name_fk = person_name.pk INNER JOIN\n"
                + "                         patient_id ON patient.patient_id_fk = patient_id.pk where dicomattrs_fk!=0) as RowConstrainedResult\n"
                + " WHERE   RowNum >= " + pagerStart + "\n"
                + "    AND RowNum < " + pagerStop + "\n"
                + "ORDER BY RowNum";
    }

    public static String getPatientDicomAttrsUpdateQuery(long dicomattrs_fk, long patientPk) {
        return "UPDATE       patient\n"
                + "SET          dicomattrs_fk = " + dicomattrs_fk + "\n"
                + " WHERE        (pk = " + patientPk + ")";
    }

    //**************************
    //STUDY*********************
    //**************************
    public static String getStudyToplamCountQuery() {
        return "SELECT    COUNT(*) as count\n"
                + "FROM           study";
    }

    public static String getStudyAktarilmamisCountQuery() {
        return "SELECT    COUNT(*) as count\n"
                + "FROM           study "
                + " WHERE dicomattrs_fk=0";
    }

    public static String getStudyAktarilmisCountQuery() {
        return "SELECT    COUNT(*) as count\n"
                + "FROM           study\n "
                + " WHERE dicomattrs_fk!=0";
    }

    public static String getStudyTumTabloQuery(long pagerStart, long pagerStop) {
        return "SELECT  *\n"
                + "FROM    ( \n"
                + "			SELECT    ROW_NUMBER() OVER ( ORDER BY pk ) AS RowNum, \n"
                + "					pk, study_date, study_time, accession_no, study_iuid, study_id\n"
                + "FROM            study) as RowConstrainedResult\n"
                + " WHERE   RowNum >= " + pagerStart + "\n"
                + "    AND RowNum < " + pagerStop + "\n"
                + "ORDER BY RowNum";
    }

    public static String getStudyAktarilmamisTabloQuery(long pagerStart, long pagerStop) {
        return "SELECT  *\n"
                + "FROM    ( \n"
                + "			SELECT    ROW_NUMBER() OVER ( ORDER BY pk ) AS RowNum, \n"
                + "					pk, study_date, study_time, accession_no, study_iuid, study_id\n"
                + "FROM            study"
                + " WHERE dicomattrs_fk=0) as RowConstrainedResult\n"
                + " WHERE   RowNum >= " + pagerStart + "\n"
                + "    AND RowNum < " + pagerStop + "\n"
                + "ORDER BY RowNum";
    }

    public static String getStudyAktarilmisTabloQuery(long pagerStart, long pagerStop) {
        return "SELECT  *\n"
                + "FROM    ( \n"
                + "			SELECT    ROW_NUMBER() OVER ( ORDER BY pk ) AS RowNum, \n"
                + "					pk, study_date, study_time, accession_no, study_iuid, study_id\n"
                + "FROM            study"
                + " WHERE dicomattrs_fk!=0) as RowConstrainedResult\n"
                + " WHERE   RowNum >= " + pagerStart + "\n"
                + "    AND RowNum < " + pagerStop + "\n"
                + "ORDER BY RowNum";
    }

    public static String getStudyDicomAttrsUpdateQuery(long dicomattrs_fk, long studyPk) {
        return "UPDATE       study\n"
                + "SET          dicomattrs_fk = " + dicomattrs_fk + "\n"
                + " WHERE        (pk = " + studyPk + ")	";
    }

    //**************************
    //SERIES********************
    //**************************
    public static String getSeriesToplamCountQuery() {
        return "SELECT    COUNT(*) as count\n"
                + "FROM           series";
    }

    public static String getSeriesAktarilmamisCountQuery() {
        return "SELECT    COUNT(*) as count\n"
                + "FROM           series "
                + " WHERE dicomattrs_fk=0";
    }

    public static String getSeriesAktarilmisCountQuery() {
        return "SELECT    COUNT(*) as count\n"
                + "FROM           series\n "
                + " WHERE dicomattrs_fk!=0";
    }

    public static String getSeriesTumTabloQuery(long pagerStart, long pagerStop) {
        return "SELECT  *\n"
                + "FROM    ( \n"
                + "			SELECT    ROW_NUMBER() OVER ( ORDER BY pk ) AS RowNum, \n"
                + "					pk, modality, series_iuid, series_no\n"
                + "FROM            series) as RowConstrainedResult\n"
                + " WHERE   RowNum >= " + pagerStart + "\n"
                + "    AND RowNum < " + pagerStop + "\n"
                + "ORDER BY RowNum";
    }

    public static String getSeriesAktarilmamisTabloQuery(long pagerStart, long pagerStop) {
        return "SELECT  *\n"
                + "FROM    ( \n"
                + "			SELECT    ROW_NUMBER() OVER ( ORDER BY pk ) AS RowNum, \n"
                + "					pk, modality, series_iuid, series_no\n"
                + "FROM            series"
                + " WHERE dicomattrs_fk=0) as RowConstrainedResult\n"
                + " WHERE   RowNum >= " + pagerStart + "\n"
                + "    AND RowNum < " + pagerStop + "\n"
                + "ORDER BY RowNum";
    }

    public static String getSeriesAktarilmisTabloQuery(long pagerStart, long pagerStop) {
        return "SELECT  *\n"
                + "FROM    ( \n"
                + "			SELECT    ROW_NUMBER() OVER ( ORDER BY pk ) AS RowNum, \n"
                + "					pk, modality, series_iuid, series_no\n"
                + "FROM            series"
                + " WHERE dicomattrs_fk!=0) as RowConstrainedResult\n"
                + " WHERE   RowNum >= " + pagerStart + "\n"
                + "    AND RowNum < " + pagerStop + "\n"
                + "ORDER BY RowNum";
    }

    public static String getSeriesDicomAttrsUpdateQuery(long dicomattrs_fk, long studyPk) {
        return "UPDATE       series\n"
                + "SET          dicomattrs_fk = " + dicomattrs_fk + "\n"
                + " WHERE        (pk = " + studyPk + ")	";
    }

    //**************************
    //INSTANCE******************
    //**************************
    public static String getInstanceToplamCountQuery() {
        return "SELECT    COUNT(*) as count\n"
                + "FROM           instance INNER JOIN\n"
                + "                         location ON instance.pk = location.instance_fk";
    }

    public static String getInstanceAktarilmamisCountQuery() {
        return "SELECT    COUNT(*) as count\n"
                + "FROM           instance INNER JOIN\n"
                + "                         location ON instance.pk = location.instance_fk where dicomattrs_fk=0";
    }

    public static String getInstanceAktarilmisCountQuery() {
        return "SELECT    COUNT(*) as count\n"
                + "FROM           instance INNER JOIN\n"
                + "                         location ON instance.pk = location.instance_fk where dicomattrs_fk!=0";
    }

    public static String getInstanceTumTabloQuery(long pagerStart, long pagerStop) {
        return "SELECT  *\n"
                + "FROM    ( \n"
                + "			SELECT    ROW_NUMBER() OVER ( ORDER BY instance.pk ) AS RowNum, \n"
                + "					instance.pk, instance.sop_cuid, instance.sop_iuid, instance.inst_no, instance.num_frames,  location.storage_path\n"
                + "\n"
                + "FROM            instance INNER JOIN\n"
                + "                         location ON instance.pk = location.instance_fk                         ) as RowConstrainedResult\n"
                + " WHERE   RowNum >=" + pagerStart + "\n"
                + "    AND RowNum < " + pagerStop + "\n"
                + "ORDER BY RowNum		";
    }

    public static String getInstanceAktarilmamisTabloQuery(long pagerStart, long pagerStop) {
        return "SELECT  *\n"
                + "FROM    ( \n"
                + "			SELECT    ROW_NUMBER() OVER ( ORDER BY instance.pk ) AS RowNum, \n"
                + "					instance.pk, instance.sop_cuid, instance.sop_iuid, instance.inst_no, instance.num_frames,  location.storage_path\n"
                + "\n"
                + "FROM            instance INNER JOIN\n"
                + "                         location ON instance.pk = location.instance_fk"
                + " WHERE dicomattrs_fk=0                          ) as RowConstrainedResult\n"
                + " WHERE   RowNum >=" + pagerStart + "\n"
                + "    AND RowNum < " + pagerStop + "\n"
                + "ORDER BY RowNum		";
    }

    public static String getInstanceAktarilmisTabloQuery(long pagerStart, long pagerStop) {
        return "SELECT  *\n"
                + "FROM    ( \n"
                + "			SELECT    ROW_NUMBER() OVER ( ORDER BY instance.pk ) AS RowNum, \n"
                + "					instance.pk, instance.sop_cuid, instance.sop_iuid, instance.inst_no, instance.num_frames,  location.storage_path\n"
                + "\n"
                + "FROM            instance INNER JOIN\n"
                + "                         location ON instance.pk = location.instance_fk"
                + " WHERE dicomattrs_fk!=0                          ) as RowConstrainedResult\n"
                + " WHERE   RowNum >=" + pagerStart + "\n"
                + "    AND RowNum < " + pagerStop + "\n"
                + "ORDER BY RowNum		";
    }

    public static String getInstanceDicomAttrsUpdateQuery(long dicomattrs_fk, long instancePk) {
        return "UPDATE       instance\n"
                + "SET          dicomattrs_fk = " + dicomattrs_fk + "\n"
                + " WHERE        (pk = " + instancePk + ")	";
    }

    //**************************
    //DICOMATTRS****************
    //**************************
    public static String getDicomAttrsMaxPkQuery() {
        return "select max(pk) as pk from dicomattrs";
    }

    public static String getDicomAttrsInsertAttributeHexBlobQuery(long pk, String hex) {
        return "SET IDENTITY_INSERT dicomattrs on "
                + " "
                + "insert into dicomattrs (pk,attrs) values (" + pk + "," + hex + ") "
                + " "
                + "SET IDENTITY_INSERT dicomattrs off "
                + "";
    }
    
    public static void main(String[] args) {
        System.out.println(getStudyAktarilmamisCountQuery());
    }
}
