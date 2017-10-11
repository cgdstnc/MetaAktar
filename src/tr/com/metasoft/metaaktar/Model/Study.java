/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tr.com.metasoft.metaaktar.Model;

/**
 * Created by Cagdas Tunca on 10.04.2017.
 */
public class Study {
    private Long pk;
    private String studyDate;
    private String studyTime;
    private String accessionNo;
    private String studyIUID;
    private String studyID;

    private String attrsBlob;

    public Study(Long pk, String studyDate, String studyTime, String accessionNo, String studyIUID, String studyID) {
        this.pk = pk;
        this.studyDate = studyDate;
        this.studyTime = studyTime;
        this.accessionNo = accessionNo;
        this.studyIUID = studyIUID;
        this.studyID = studyID;
    }

    public Long getPk() {
        return pk;
    }

    public void setPk(Long pk) {
        this.pk = pk;
    }

    public String getStudyDate() {
        return studyDate;
    }

    public void setStudyDate(String studyDate) {
        this.studyDate = studyDate;
    }

    public String getStudyTime() {
        return studyTime;
    }

    public void setStudyTime(String studyTime) {
        this.studyTime = studyTime;
    }

    public String getAccessionNo() {
        return accessionNo;
    }

    public void setAccessionNo(String accessionNo) {
        this.accessionNo = accessionNo;
    }

    public String getStudyIUID() {
        return studyIUID;
    }

    public void setStudyIUID(String studyIUID) {
        this.studyIUID = studyIUID;
    }

    public String getStudyID() {
        return studyID;
    }

    public void setStudyID(String studyID) {
        this.studyID = studyID;
    }

    public String getAttrsBlob() {
        return attrsBlob;
    }

    public void setAttrsBlob(String attrsBlob) {
        this.attrsBlob = attrsBlob;
    }

    @Override
    public String toString() {
        return "Study{" +
                "pk=" + pk +
                ", studyDate='" + studyDate + '\'' +
                ", studyTime='" + studyTime + '\'' +
                ", accessionNo='" + accessionNo + '\'' +
                ", studyIUID='" + studyIUID + '\'' +
                ", studyID='" + studyID + '\'' +
                ", attrsBlob='" + attrsBlob + '\'' +
                '}';
    }
}
