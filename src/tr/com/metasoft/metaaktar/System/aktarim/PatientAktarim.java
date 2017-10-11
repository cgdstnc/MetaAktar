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
import tr.com.metasoft.metaaktar.System.database.DatabaseOperations;
import tr.com.metasoft.metaaktar.System.publicEnum.KayitTipi;
import tr.com.metasoft.metaaktar.System.utils;

/**
 *
 * @author Administrator
 */
public class PatientAktarim implements Aktarim {

    private KayitTipi tip;
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

    public PatientAktarim(KayitTipi tip, long dicomattrsPkStart, long pagerSize) {
        this.tip = tip;
        this.dicomattrsPkStart = dicomattrsPkStart;
        this.pagerSize = pagerSize;

        logFileName = dateFormat.format(new Date()) + "_PatientAktarim.log";
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
            System.out.println("patient - Aktarim baslamis zaten");
            return;
        }

        startTime = System.currentTimeMillis();

        try {
            thread = new Thread(new Aktarici());
            thread.start();

        } catch (Exception ex) {
            Logger.getLogger(PatientAktarim.class.getName()).log(Level.SEVERE, null, ex);
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
                int patientCount = DatabaseOperations.getPatientCount(tip);
                LinkedList<Patient> patients = new LinkedList<Patient>();
                if (useProgress) {
                    try {
                        progessbar.setMaximum(patientCount);
                        progessbar.setValue(0);
                        jlTotal.setText(patientCount + "");
                    } catch (Exception e) {
                    }
                }

                for (int i = 0; i < patientCount; i += pagerSize) {
                    while (pause) {
                        Thread.sleep(1000);
                    }
                    if (useProgress) {
                        try {
                            progessbar.setValue((int) (i + pagerSize));
                            int tmp = (int) (((i + pagerSize) <= patientCount) ? (i + pagerSize) : patientCount);

                            jlCurrent.setText(tmp + "");//pagerSize
                        } catch (Exception e) {
                        }
                    }

                    patients.removeAll(patients);

                    ResultSet rs = DatabaseOperations.getPatientTable(tip, 0, pagerSize);//i, i + pagerSize
                    //tablodaki her kayit icin nesne olustur hex degerini hesapla listeye ekle
                    while (rs.next()) {
                        try {
                            Patient patient = new Patient(rs.getLong("pk"), rs.getString("family_name"), rs.getString("given_name"), rs.getString("pat_id"), rs.getString("pat_birthdate"), rs.getString("pat_sex"), "Aktarim:" + System.currentTimeMillis() + ",\nYorum:" + rs.getString("pat_custom1"));
                            String blob = "0x" + utils.bytesToHex(utils.attributesToByteArray(utils.generateFirstAttributesBlob("ISO_IR 148", patient.getName(), patient.getPatientId(), patient.getPatientBirthDate(), patient.getPatientSex(), patient.getPatientComments())));
                            patient.setAttrsBlob(blob);
                            patients.add(patient);
                        } catch (Exception e) {
                            e.printStackTrace();
                            try {
                                utils.appendLog(logFileName, "**************************");
                                utils.appendLog(logFileName, "<PatientAktarim" + dateFormat.format(new Date()) + "> " + "PAGER START bu hata sirasinda su indexteydi:" + i);
                                utils.appendLog(logFileName, "<PatientAktarim" + dateFormat.format(new Date()) + "> " + e.getLocalizedMessage());
                                utils.appendLog(logFileName, "<PatientAktarim" + dateFormat.format(new Date()) + "> " + e.toString());
                            } catch (Exception e1) {
                            }
                        }
                    }
                    // Listeyi dolas dicomAttrs tablosuna dicomattrsPkStart dan baslayarak ekle ve degeri arttır.
                    // Sonra dicomAttrs'ın pk sını patient tablosundaki dicomAttrsFk ya yaz
                    for (Patient patient : patients) {
                        try {
                            DatabaseOperations.insertAttributeHexBlob(dicomattrsPkStart, patient.getAttrsBlob());
                            dicomattrsPkStart++;
                            DatabaseOperations.updateDicomAttrsFkForPatient(patient.getPk(), dicomattrsPkStart - 1);
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.err.println(patient.toString());
                            try {
                                utils.appendLog(logFileName, "**************************");
                                utils.appendLog(logFileName, "<PatientAktarim" + dateFormat.format(new Date()) + "> " + "PAGER START bu hata sirasinda su indexteydi:" + i);
                                utils.appendLog(logFileName, "<PatientAktarim" + dateFormat.format(new Date()) + "> " + patient.toString());
                                utils.appendLog(logFileName, "<PatientAktarim" + dateFormat.format(new Date()) + "> " + e.getLocalizedMessage());
                                utils.appendLog(logFileName, "<PatientAktarim" + dateFormat.format(new Date()) + "> " + e.toString());
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
                Logger.getLogger(PatientAktarim.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        DatabaseOperations.connect("192.168.12.132", "1433", "PACSDB1", "sa", "meta26.soft");
        System.out.println();
        PatientAktarim p = new PatientAktarim(KayitTipi.HEPSI, DatabaseOperations.getDicomAttrsMaxPk() + 1, 1);
        p.aktar();
        p.duraklat();
        Thread.sleep(5000);
        p.devamEttir();
    }
}
