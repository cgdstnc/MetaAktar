/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tr.com.metasoft.metaaktar.Model;


/**
 * Created by Cagdas Tunca on 10.04.2017.
 */
public class Patient {
    private Long pk;
    private String family_name;
    private String given_name;
    private String name;
    private String patientId;
    private String patientBirthDate;
    private String patientSex;
    private String patientComments;

    private String attrsBlob;

    public Patient(Long pk, String family_name, String given_name, String patientId, String patientBirthDate, String patientSex, String patientComments) {
        this.pk = pk;
        this.family_name = family_name;
        this.given_name = given_name;
        this.patientId = patientId;
        this.patientBirthDate = patientBirthDate;
        this.patientSex = patientSex;
        this.patientComments = patientComments;

        String str = family_name == null ? "" : family_name;
        String str2 = given_name == null ? "" : given_name;
        this.name = str + str2;
    }

    public Long getPk() {
        return pk;
    }

    public void setPk(Long pk) {
        this.pk = pk;
    }

    public String getFamily_name() {
        return family_name;
    }

    public void setFamily_name(String family_name) {
        this.family_name = family_name;
    }

    public String getGiven_name() {
        return given_name;
    }

    public void setGiven_name(String given_name) {
        this.given_name = given_name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPatientId() {
        return patientId;
    }

    public void setPatientId(String patientId) {
        this.patientId = patientId;
    }

    public String getPatientBirthDate() {
        return patientBirthDate;
    }

    public void setPatientBirthDate(String patientBirthDate) {
        this.patientBirthDate = patientBirthDate;
    }

    public String getPatientSex() {
        return patientSex;
    }

    public void setPatientSex(String patientSex) {
        this.patientSex = patientSex;
    }

    public String getPatientComments() {
        return patientComments;
    }

    public void setPatientComments(String patientComments) {
        this.patientComments = patientComments;
    }

    public String getAttrsBlob() {
        return attrsBlob;
    }

    public void setAttrsBlob(String attrsBlob) {
        this.attrsBlob = attrsBlob;
    }


    @Override
    public String toString() {
        return "Patient{" +
                "pk=" + pk +
                ", family_name='" + family_name + '\'' +
                ", given_name='" + given_name + '\'' +
                ", name='" + name + '\'' +
                ", patientId='" + patientId + '\'' +
                ", patientBirthDate='" + patientBirthDate + '\'' +
                ", patientSex='" + patientSex + '\'' +
                ", patientComments='" + patientComments + '\'' +
                ", attrsBlob='" + attrsBlob + '\'' +
                '}';
    }
}
