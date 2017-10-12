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
import tr.com.metasoft.metaaktar.Model.Serie;
import tr.com.metasoft.metaaktar.Model.Study;
import tr.com.metasoft.metaaktar.System.database.DatabaseOperations;
import tr.com.metasoft.metaaktar.System.publicEnum;
import tr.com.metasoft.metaaktar.System.utils;

/**
 *
 * @author Administrator
 */
public class SeriesAktarim implements Aktarim {

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

    public SeriesAktarim(publicEnum.KayitTipi tip, long dicomattrsPkStart, long pagerSize) {
        this.tip = tip;
        this.dicomattrsPkStart = dicomattrsPkStart;
        this.pagerSize = pagerSize;

        logFileName = dateFormat.format(new Date()) + "_SeriesAktarim.log";
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
            System.out.println("series - Aktarim baslamis zaten");
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

    public boolean isPaused() {
        return pause;
    }

    private class Aktarici implements Runnable {

        @Override
        public void run() {
            try {
                int seriesCount = DatabaseOperations.getSeriesCount(tip);
                LinkedList<Serie> series = new LinkedList<Serie>();
                if (useProgress) {
                    try {
                        progessbar.setMaximum(seriesCount);
                        progessbar.setValue(0);
                        jlTotal.setText(seriesCount + "");
                    } catch (Exception e) {
                    }
                }

                int pagerStart = 0;

                for (int i = 0; i < seriesCount; i += pagerSize) {
                    while (pause) {
                        Thread.sleep(1000);
                    }
                    if (useProgress) {
                        try {
                            progessbar.setValue((int) (i + pagerSize));
                            int tmp = (int) (((i + pagerSize) <= seriesCount) ? (i + pagerSize) : seriesCount);

                            jlCurrent.setText(tmp + "");//pagerSize
                        } catch (Exception e) {
                        }
                    }

                    series.removeAll(series);

                    ResultSet rs = DatabaseOperations.getSeriesTable(tip, pagerStart, pagerStart + pagerSize);
                    //tablodaki her kayit icin nesne olustur hex degerini hesapla listeye ekle
                    while (rs.next()) {
                        try {
                            Serie serie = new Serie(rs.getLong("pk"), rs.getString("modality"), rs.getString("series_iuid"), rs.getInt("series_no"));
                            String blob = "0x" + utils.bytesToHex(utils.attributesToByteArray(utils.generateThirdAttributesBlob("ISO_IR 148", serie.getModality(), serie.getSeries_iuid(), serie.getSeries_no())));
                            serie.setBlob(blob);
                            series.add(serie);
                        } catch (Exception e) {
                            e.printStackTrace();
                            try {
                                utils.appendLog(logFileName, "**************************");
                                utils.appendLog(logFileName, "<SeriesAktarim" + dateFormat.format(new Date()) + "> " + "PAGER START bu hata sirasinda su indexteydi:" + i);
                                utils.appendLog(logFileName, "<SeriesAktarim" + dateFormat.format(new Date()) + "> " + e.getLocalizedMessage());
                                utils.appendLog(logFileName, "<SeriesAktarim" + dateFormat.format(new Date()) + "> " + e.toString());
                            } catch (Exception e1) {
                            }
                        }
                    }
                    int debug = pagerStart;
                    pagerStart += (int) (pagerSize - series.size());
                    if (debug != pagerStart) {
                        System.out.println("series: debugPagerStart:" + debug);
                        System.out.println("series: PagerStart:" + pagerStart);
                        System.out.println("series: getTable(from,to):" + pagerStart + "," + (pagerStart + pagerSize));

                    }
                    // Listeyi dolas dicomAttrs tablosuna dicomattrsPkStart dan baslayarak ekle ve degeri arttır.
                    // Sonra dicomAttrs'ın pk sını patient tablosundaki dicomAttrsFk ya yaz
                    for (Serie serie : series) {
                        try {
                            DatabaseOperations.insertAttributeHexBlob(dicomattrsPkStart, serie.getBlob());
                            dicomattrsPkStart++;
                            DatabaseOperations.updateDicomAttrsFkForSeries(serie.getPk(), dicomattrsPkStart - 1);
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.err.println(serie.toString());
                            try {
                                utils.appendLog(logFileName, "**************************");
                                utils.appendLog(logFileName, "<SeriesAktarim" + dateFormat.format(new Date()) + "> " + "PAGER START bu hata sirasinda su indexteydi:" + i);
                                utils.appendLog(logFileName, "<SeriesAktarim" + dateFormat.format(new Date()) + "> " + serie.toString());
                                utils.appendLog(logFileName, "<SeriesAktarim" + dateFormat.format(new Date()) + "> " + e.getLocalizedMessage());
                                utils.appendLog(logFileName, "<SeriesAktarim" + dateFormat.format(new Date()) + "> " + e.toString());
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
                Logger.getLogger(SeriesAktarim.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
