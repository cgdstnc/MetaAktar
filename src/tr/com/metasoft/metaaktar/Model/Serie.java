/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tr.com.metasoft.metaaktar.Model;

/**
 * Created by Cagdas Tunca on 10.04.2017.
 */
public class Serie {
    private Long pk;
    private String modality;
    private String series_iuid;
    private Integer series_no;

    private String blob;

    public Serie(Long pk, String modality, String series_iuid, Integer series_no) {
        this.pk = pk;
        this.modality = modality;
        this.series_iuid = series_iuid;
        this.series_no = series_no;
    }

    public Long getPk() {
        return pk;
    }

    public void setPk(Long pk) {
        this.pk = pk;
    }

    public String getModality() {
        return modality;
    }

    public void setModality(String modality) {
        this.modality = modality;
    }

    public String getSeries_iuid() {
        return series_iuid;
    }

    public void setSeries_iuid(String series_iuid) {
        this.series_iuid = series_iuid;
    }

    public int getSeries_no() {
        return series_no;
    }

    public void setSeries_no(Integer series_no) {
        this.series_no = series_no;
    }

    public String getBlob() {
        return blob;
    }

    public void setBlob(String blob) {
        this.blob = blob;
    }

    @Override
    public String toString() {
        return "Serie{" +
                "pk=" + pk +
                ", modality='" + modality + '\'' +
                ", series_iuid='" + series_iuid + '\'' +
                ", series_no=" + series_no +
                ", blob='" + blob + '\'' +
                '}';
    }
}