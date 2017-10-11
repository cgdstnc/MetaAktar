/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tr.com.metasoft.metaaktar.System;

/**
 *
 * @author Administrator
 */
public class publicEnum {

    public static enum KayitTipi {
        HEPSI(),
        AKTARILMIS(),
        AKTARILMAMIS();
    }

    public static enum dbTable {
        Patient(),
        Study(),
        Serie(),
        Instance(),
        DicomAttrs();
    }

}
