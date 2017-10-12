/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tr.com.metasoft.metaaktar.System.aktarim;

import java.io.File;
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
import org.dcm4che3.data.Attributes;
import tr.com.metasoft.metaaktar.Model.Instance;
import tr.com.metasoft.metaaktar.Model.Study;
import tr.com.metasoft.metaaktar.System.database.DatabaseOperations;
import tr.com.metasoft.metaaktar.System.publicEnum;
import tr.com.metasoft.metaaktar.System.utils;

/**
 *
 * @author Administrator
 */
public class InstanceAktarim implements Aktarim {

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

    private String baseFilePath;

    public InstanceAktarim(publicEnum.KayitTipi tip, long dicomattrsPkStart, long pagerSize, String baseFilePath) {
        this.tip = tip;
        this.dicomattrsPkStart = dicomattrsPkStart;
        this.pagerSize = pagerSize;
        this.baseFilePath = baseFilePath;

        logFileName = dateFormat.format(new Date()) + "_InstanceAktarim.log";
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
            System.out.println("instance - Aktarim baslamis zaten");
            return;
        }
        startTime = System.currentTimeMillis();

        try {
            thread = new Thread(new Aktarici());
            thread.start();

        } catch (Exception ex) {
            Logger.getLogger(InstanceAktarim.class.getName()).log(Level.SEVERE, null, ex);
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

    public boolean isPaused() {
        return pause;
    }

    private class Aktarici implements Runnable {

        @Override
        public void run() {
            try {
                int instanceCount = DatabaseOperations.getInstanceCount(tip);
                LinkedList<Instance> instances = new LinkedList<Instance>();
                if (useProgress) {
                    try {
                        progessbar.setMaximum(instanceCount);
                        progessbar.setValue(0);
                        jlTotal.setText(instanceCount + "");
                    } catch (Exception e) {
                    }
                }
                int pagerStart = 0;

                for (int i = 0; i < instanceCount; i += pagerSize) {
                    while (pause) {
                        Thread.sleep(1000);
                    }
                    if (useProgress) {
                        try {
                            progessbar.setValue((int) (i + pagerSize));
                            int tmp = (int) (((i + pagerSize) <= instanceCount) ? (i + pagerSize) : instanceCount);

                            jlCurrent.setText(tmp + "");//pagerSize
                        } catch (Exception e) {
                        }
                    }

                    instances.removeAll(instances);

                    ResultSet rs = DatabaseOperations.getInstanceTable(tip, pagerStart, pagerStart + pagerSize);//i, i + pagerSize
                    //tablodaki her kayit icin nesne olustur hex degerini hesapla listeye ekle
                    while (rs.next()) {
                        try {
                            Instance instance = new Instance(rs.getLong("pk"), rs.getString("sop_cuid"), rs.getString("sop_iuid"), rs.getInt("inst_no"), rs.getInt("num_frames"), -1/*rs.getInt("Rows")*/, -1/*rs.getInt("Columns")*/, rs.getString("storage_path"));
                            //System.out.println(instance.getPk());

                            File f = new File(baseFilePath + instance.getStorage_path().replaceAll("/", "\\\\"));
                            if (f.exists()) {
                                // System.out.println(f.getAbsolutePath());
                                Attributes read = utils.readInstanceRelatedAttributes(f);
                                String blob = "0x" + utils.bytesToHex(utils.attributesToByteArray(read));
                                instance.setBlob(blob);
                                instances.add(instance);

                            } else {

                                try {
                                    utils.appendLog(logFileName, "**************************");
                                    utils.appendLog(logFileName, "<InstanceAktarim" + dateFormat.format(new Date()) + "> " + "DOSYA BULUNAMADI ->" + baseFilePath + instance.getStorage_path().replaceAll("/", "\\\\"));
                                    utils.appendLog(logFileName, "<InstanceAktarim" + dateFormat.format(new Date()) + "> " + "PAGER START bu hata sirasinda su indexteydi:" + i);
                                    utils.appendLog(logFileName, "<InstanceAktarim" + dateFormat.format(new Date()) + "> " + "instance :" + instance.toString());
                                } catch (Exception e) {
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            try {
                                utils.appendLog(logFileName, "**************************");
                                utils.appendLog(logFileName, "<InstanceAktarim" + dateFormat.format(new Date()) + "> " + "PAGER START bu hata sirasinda su indexteydi:" + i);
                                utils.appendLog(logFileName, "<InstanceAktarim" + dateFormat.format(new Date()) + "> " + e.getLocalizedMessage());
                                utils.appendLog(logFileName, "<InstanceAktarim" + dateFormat.format(new Date()) + "> " + e.toString());
                            } catch (Exception e1) {
                            }
                        }
                    }

                    int debug = pagerStart;
                    pagerStart += (int) (pagerSize - instances.size());
                    if (debug != pagerStart) {
                        System.out.println("instances: debugPagerStart:" + debug);
                        System.out.println("instances: PagerStart:" + pagerStart);
                        System.out.println("instances: getTable(from,to):" + pagerStart + "," + (pagerStart + pagerSize));

                    }
                    // Listeyi dolas dicomAttrs tablosuna dicomattrsPkStart dan baslayarak ekle ve degeri arttır.
                    // Sonra dicomAttrs'ın pk sını patient tablosundaki dicomAttrsFk ya yaz
                    for (Instance instance : instances) {
                        try {
                            DatabaseOperations.insertAttributeHexBlob(dicomattrsPkStart, instance.getBlob());
                            dicomattrsPkStart++;
                            DatabaseOperations.updateDicomAttrsFkForInstance(instance.getPk(), dicomattrsPkStart - 1);
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.err.println(instance.toString());
                            try {
                                utils.appendLog(logFileName, "**************************");
                                utils.appendLog(logFileName, "<InstanceAktarim" + dateFormat.format(new Date()) + "> " + "PAGER START bu hata sirasinda su indexteydi:" + i);
                                utils.appendLog(logFileName, "<InstanceAktarim" + dateFormat.format(new Date()) + "> " + e.getLocalizedMessage());
                                utils.appendLog(logFileName, "<InstanceAktarim" + dateFormat.format(new Date()) + "> " + e.toString());
                                utils.appendLog(logFileName, "<InstanceAktarim" + dateFormat.format(new Date()) + "> " + instance.toString());

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
                Logger.getLogger(InstanceAktarim.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

}
