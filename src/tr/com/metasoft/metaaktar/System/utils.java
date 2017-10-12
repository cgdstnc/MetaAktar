/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tr.com.metasoft.metaaktar.System;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.data.Tag;
import org.dcm4che3.data.UID;
import org.dcm4che3.data.VR;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.io.DicomOutputStream;

import java.io.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by Cagdas Tunca on 05.04.2017.
 */
public class utils {

    final static protected char[] hexArray = "0123456789ABCDEF".toCharArray();

    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    public static byte[] attributesToByteArray(Attributes attrs) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(512);
        try {
            DicomOutputStream dos = new DicomOutputStream(out, UID.ExplicitVRLittleEndian);
            dos.writeDataset(null, attrs);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return out.toByteArray();
    }

    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static Attributes decodeAttributes(byte[] b) {
        Attributes result = new Attributes();
        ByteArrayInputStream is = new ByteArrayInputStream(b);
        try {
            DicomInputStream dis = new DicomInputStream(is);
            dis.readFileMetaInformation();
            dis.readAttributes(result, -1, -1);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static Attributes generateFirstAttributesBlob(String SpecificCharacterSet, String PatientName, String PatientID, String PatientBirthDate, String PatientSex, String PatientComments) {
        Attributes pk1 = new Attributes();
        pk1.setString(Tag.SpecificCharacterSet, VR.CS, SpecificCharacterSet);
        pk1.setString(Tag.PatientName, VR.PN, PatientName);
        pk1.setString(Tag.PatientID, VR.LO, PatientID);
        pk1.setString(Tag.PatientBirthDate, VR.DA, PatientBirthDate);
        pk1.setString(Tag.PatientSex, VR.CS, PatientSex);
        pk1.setString(Tag.PatientComments, VR.LT, PatientComments);

        return pk1;
    }

    public static Attributes generateSecondAttributesBlob(String SpecificCharacterSet, String StudyDate, String StudyTime, String AccessionNumber, String ReferringPhysicianName, String PatientAge, String StudyInstanceUID, String StudyID) {
        Attributes pk2 = new Attributes();
        pk2.setString(Tag.SpecificCharacterSet, VR.CS, SpecificCharacterSet);
        pk2.setString(Tag.StudyDate, VR.DA, StudyDate);
        pk2.setString(Tag.StudyTime, VR.TM, StudyTime);
        pk2.setString(Tag.AccessionNumber, VR.SH, AccessionNumber);
        pk2.setString(Tag.ReferringPhysicianName, VR.PN, ReferringPhysicianName);
        pk2.setString(Tag.PatientAge, VR.AS, PatientAge);
        pk2.setString(Tag.StudyInstanceUID, VR.UI, StudyInstanceUID);
        pk2.setString(Tag.StudyID, VR.SH, StudyID);

        return pk2;
    }

    public static Attributes generateThirdAttributesBlob(String SpecificCharacterSet, String Modality, String SeriesInstanceUID, int SeriesNumber) {
        Attributes pk3 = new Attributes();
        pk3.setString(Tag.SpecificCharacterSet, VR.CS, SpecificCharacterSet);
        pk3.setString(Tag.Modality, VR.CS, Modality);
        pk3.setString(Tag.SeriesInstanceUID, VR.UI, SeriesInstanceUID);
        pk3.setInt(Tag.SeriesNumber, VR.IS, SeriesNumber);

        return pk3;
    }

    public static Attributes generateFourthAttributesBlob(String SpecificCharacterSet, String SOPClassUID, String SOPInstanceUID, int InstanceNumber, String NumberOfFrames, String Rows, String Columns, String BitsAllocated) {
        Attributes pk4 = new Attributes();
        pk4.setString(Tag.SpecificCharacterSet, VR.CS, SpecificCharacterSet);
        pk4.setString(Tag.SOPClassUID, VR.UI, SOPClassUID);
        pk4.setString(Tag.SOPInstanceUID, VR.UI, SOPInstanceUID);
        pk4.setInt(Tag.InstanceNumber, VR.IS, InstanceNumber);
        pk4.setString(Tag.NumberOfFrames, VR.IS, NumberOfFrames);
        pk4.setString(Tag.Rows, VR.US, Rows);
        pk4.setString(Tag.Columns, VR.US, Columns);
        pk4.setString(Tag.BitsAllocated, VR.US, BitsAllocated);

        return pk4;
    }

    public static Attributes readInstanceRelatedAttributes(File file) throws IOException {
        DicomInputStream dis = new DicomInputStream(file);
        Attributes attrs = dis.readDataset(-1, -1);
//        return attrs.getString(Tag.BitsAllocated);
//        String[] ret = {attrs.getString(Tag.SOPInstanceUID), attrs.getString(Tag.BitsAllocated), attrs.getString(Tag.Rows), attrs.getString(Tag.Columns)};
//        return ret;

        Attributes instanceAttrs = new Attributes();
        instanceAttrs.setString(Tag.SpecificCharacterSet, VR.CS, "ISO_IR 148" /*attrs.getString(Tag.SpecificCharacterSet)*/);
        instanceAttrs.setString(Tag.SOPClassUID, VR.UI, attrs.getString(Tag.SOPClassUID));
        instanceAttrs.setString(Tag.SOPInstanceUID, VR.UI, attrs.getString(Tag.SOPInstanceUID));
        String instNo = (attrs.getString(Tag.InstanceNumber) == null) ? "1" : attrs.getString(Tag.InstanceNumber);
        instanceAttrs.setInt(Tag.InstanceNumber, VR.IS, Integer.valueOf(instNo));
        instanceAttrs.setString(Tag.NumberOfFrames, VR.IS, attrs.getString(Tag.NumberOfFrames));
        instanceAttrs.setString(Tag.Rows, VR.US, attrs.getString(Tag.Rows));
        instanceAttrs.setString(Tag.Columns, VR.US, attrs.getString(Tag.Columns));
        instanceAttrs.setString(Tag.BitsAllocated, VR.US, attrs.getString(Tag.BitsAllocated));
        try {
            
            dis.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return instanceAttrs;
    }

    public static String getBitsAllocated(File file) throws IOException {
        DicomInputStream dis = new DicomInputStream(file);
        Attributes attrs = dis.readDataset(-1, -1);
        return attrs.getString(Tag.BitsAllocated);
    }

    //region LOG
    public static void appendLog(String FileName, String msg) throws IOException {
        File Log = new File(FileName);
        FileWriter fileWriter = new FileWriter(Log, true);
        fileWriter.append("\n" + msg);
        fileWriter.flush();
        fileWriter.close();
    }

    //endregion
    public static String convertMilisecondToTime(long millis) {
        return String.format("%02d dk, %02d sn",
                TimeUnit.MILLISECONDS.toMinutes(millis),
                TimeUnit.MILLISECONDS.toSeconds(millis)
                - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis))
        );
    }

    public static void main(String[] args) {
        String hex = "0x0800050043530A0049534F5F4952203134380800160055491A00312E322E3834302E31303030382E352E312E342E312E312E37000800180055492200312E322E3834302E31303030382E32303136303730393231353934313935322E300020001300495302003020280008004953020031202800100055530200D6012800110055530200600228000001555302000800";
        hex = hex.replaceAll("0x", "");
        System.out.println(decodeAttributes(hexStringToByteArray(hex)));
    }
}
