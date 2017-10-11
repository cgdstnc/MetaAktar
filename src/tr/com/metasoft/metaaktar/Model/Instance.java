/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tr.com.metasoft.metaaktar.Model;


/**
 * Created by Cagdas Tunca on 10.04.2017.
 */
public class Instance {

    private Long pk;
    private String sop_cuid;
    private String sop_iuid;
    private Integer inst_no;
    private Integer num_frames;
    private Integer rows;
    private Integer columns;
    private int bitsAllocated;
    private String storage_path;

    private String blob;

    public Instance(Long pk, String sop_cuid, String sop_iuid, Integer inst_no, Integer num_frames, Integer rows, Integer columns, String storage_path) {
        this.pk = pk;
        this.sop_cuid = sop_cuid;
        this.sop_iuid = sop_iuid;
        this.inst_no = inst_no;
        this.num_frames = num_frames;
        this.rows = rows;
        this.columns = columns;
       // this.bitsAllocated = bitsAllocated;
        this.storage_path = storage_path;
    }

    public Long getPk() {
        return pk;
    }

    public void setPk(Long pk) {
        this.pk = pk;
    }

    public String getSop_cuid() {
        return sop_cuid;
    }

    public void setSop_cuid(String sop_cuid) {
        this.sop_cuid = sop_cuid;
    }

    public String getSop_iuid() {
        return sop_iuid;
    }

    public void setSop_iuid(String sop_iuid) {
        this.sop_iuid = sop_iuid;
    }

    public Integer getInst_no() {
        return inst_no;
    }

    public void setInst_no(Integer inst_no) {
        this.inst_no = inst_no;
    }

    public Integer getNum_frames() {
        return num_frames;
    }

    public void setNum_frames(Integer num_frames) {
        this.num_frames = num_frames;
    }

    public Integer getRows() {
        return rows;
    }

    public void setRows(Integer rows) {
        this.rows = rows;
    }

    public Integer getColumns() {
        return columns;
    }

    public void setColumns(Integer columns) {
        this.columns = columns;
    }

    public int getBitsAllocated() {
        return bitsAllocated;
    }

    public void setBitsAllocated(int bitsAllocated) {
        this.bitsAllocated = bitsAllocated;
    }

    public String getStorage_path() {
        return storage_path;
    }

    public void setStorage_path(String storage_path) {
        this.storage_path = storage_path;
    }

    public String getBlob() {
        return blob;
    }

    public void setBlob(String blob) {
        this.blob = blob;
    }

    @Override
    public String toString() {
        return "Instance{" +
                "pk=" + pk +
                ", sop_cuid='" + sop_cuid + '\'' +
                ", sop_iuid='" + sop_iuid + '\'' +
                ", inst_no=" + inst_no +
                ", num_frames=" + num_frames +
                ", rows=" + rows +
                ", columns=" + columns +
                ", bitsAllocated=" + bitsAllocated +
                ", storage_path='" + storage_path + '\'' +
                ", blob='" + blob + '\'' +
                '}';
    }
}

