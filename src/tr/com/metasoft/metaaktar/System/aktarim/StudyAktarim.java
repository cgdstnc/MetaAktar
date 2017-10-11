/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tr.com.metasoft.metaaktar.System.aktarim;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JLabel;
import javax.swing.JProgressBar;
import tr.com.metasoft.metaaktar.Model.Patient;
import tr.com.metasoft.metaaktar.Model.Study;
import tr.com.metasoft.metaaktar.System.database.DatabaseOperations;
import tr.com.metasoft.metaaktar.System.publicEnum;
import tr.com.metasoft.metaaktar.System.utils;

/**
 *
 * @author Administrator
 */
public class StudyAktarim implements Aktarim {

    private publicEnum.KayitTipi tip;
    private long dicomattrsPkStart;
    private long pagerSize;

    private JProgressBar progessbar;
    private JLabel jlCurrent;
    private JLabel jlTotal;
    private boolean useProgress = false;

    private String logFileName;
    private DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HH_mm");

    private Long startTime;
    private boolean pause = false;
    private Thread thread;

    public StudyAktarim(publicEnum.KayitTipi tip, long dicomattrsPkStart, long pagerSize) {
        this.tip = tip;
        this.dicomattrsPkStart = dicomattrsPkStart;
        this.pagerSize = pagerSize;

        logFileName = dateFormat.format(new Date()) + "_StudyAktarim.log";
    }

    public void setProgressObjects(JProgressBar progessbar, JLabel jlCurrent, JLabel jlTotal) {
        this.progessbar = progessbar;
        this.jlCurrent = jlCurrent;
        this.jlTotal = jlTotal;
        useProgress = true;
    }

    @Override
    public void aktar() {
        if (startTime != null) {
            System.out.println("study - Aktarim baslamis zaten");
            return;
        }
        startTime = System.currentTimeMillis();

        try {
            thread = new Thread(new Aktarici());
            thread.start();

        } catch (Exception ex) {
            Logger.getLogger(SeriesAktarim.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void duraklat() {
        pause = true;
    }

    @Override
    public void devamEttir() {
        pause = false;
    }

    private class Aktarici implements Runnable {

        @Override
        public void run() {
            try {
                int studyCount = DatabaseOperations.getStudyCount(tip);
                LinkedList<Study> studies = new LinkedList<Study>();
                if (useProgress) {
                    try {
                        progessbar.setMaximum(studyCount);
                        progessbar.setValue(0);
                        jlTotal.setText(studyCount + "");
                    } catch (Exception e) {
                    }
                }

                for (int i = 0; i < studyCount; i += pagerSize) {
                    while (pause) {
                        Thread.sleep(1000);
                    }
                    if (useProgress) {
                        try {
                            progessbar.setValue((int) (i + pagerSize));
                            int tmp = (int) (((i + pagerSize) <= studyCount) ? (i + pagerSize) : studyCount);

                            jlCurrent.setText(tmp + "");//pagerSize
                        } catch (Exception e) {
                        }
                    }

                    studies.removeAll(studies);

                    ResultSet rs = DatabaseOperations.getStudyTable(tip, 0, pagerSize);
                    //tablodaki her kayit icin nesne olustur hex degerini hesapla listeye ekle
                    while (rs.next()) {
                        try {
                            Study study = new Study(rs.getLong("pk"), rs.getString("study_date"), rs.getString("study_time"), rs.getString("accession_no"), rs.getString("study_iuid"), rs.getString("study_id"));
                            String blob = "0x" + utils.bytesToHex(utils.attributesToByteArray(utils.generateSecondAttributesBlob("ISO_IR 148", study.getStudyDate(), study.getStudyTime(), study.getAccessionNo(), "cgdstnc_Aktarim", "0", study.getStudyIUID(), study.getStudyID())));
                            study.setAttrsBlob(blob);
                            studies.add(study);
                        } catch (Exception e) {
                            e.printStackTrace();
                            try {
                                utils.appendLog(logFileName, "**************************");
                                utils.appendLog(logFileName, "<StudyAktarim" + dateFormat.format(new Date()) + "> " + "PAGER START bu hata sirasinda su indexteydi:" + i);
                                utils.appendLog(logFileName, "<StudyAktarim" + dateFormat.format(new Date()) + "> " + e.getLocalizedMessage());
                                utils.appendLog(logFileName, "<StudyAktarim" + dateFormat.format(new Date()) + "> " + e.toString());
                            } catch (Exception e1) {
                            }
                        }
                    }
                    // Listeyi dolas dicomAttrs tablosuna dicomattrsPkStart dan baslayarak ekle ve degeri arttır.
                    // Sonra dicomAttrs'ın pk sını patient tablosundaki dicomAttrsFk ya yaz
                    for (Study study : studies) {
                        try {
                            DatabaseOperations.insertAttributeHexBlob(dicomattrsPkStart, study.getAttrsBlob());
                            dicomattrsPkStart++;
                            DatabaseOperations.updateDicomAttrsFkForStudy(study.getPk(), dicomattrsPkStart - 1);
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.err.println(study.toString());
                            try {
                                utils.appendLog(logFileName, "**************************");
                                utils.appendLog(logFileName, "<StudyAktarim" + dateFormat.format(new Date()) + "> " + "PAGER START bu hata sirasinda su indexteydi:" + i);
                                utils.appendLog(logFileName, "<StudyAktarim" + dateFormat.format(new Date()) + "> " + e.getLocalizedMessage());
                                utils.appendLog(logFileName, "<StudyAktarim" + dateFormat.format(new Date()) + "> " + e.toString());
                                utils.appendLog(logFileName, "<StudyAktarim" + dateFormat.format(new Date()) + "> " + study.toString());
                            } catch (Exception e1) {
                            }
                        }
                    }

                }
                utils.appendLog(logFileName + "_TIME", "StartTime " + startTime);
                utils.appendLog(logFileName + "_TIME", "FinishTime " + System.currentTimeMillis());
                utils.appendLog(logFileName + "_TIME", "Total " + utils.convertMilisecondToTime(System.currentTimeMillis() - startTime));

            } catch (SQLException ex) {
                Logger.getLogger(PatientAktarim.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InterruptedException ex) {
                Logger.getLogger(PatientAktarim.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(StudyAktarim.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

}
