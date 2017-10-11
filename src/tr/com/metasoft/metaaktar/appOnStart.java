/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tr.com.metasoft.metaaktar;

import tr.com.metasoft.metaaktar.View.Frame;

/**
 *
 * @author Administrator
 */
public class appOnStart {

    public static void main(String[] args) {
        try {
            for (javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    javax.swing.UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (Exception ex) {
            java.util.logging.Logger.getLogger(appOnStart.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        Frame f = new Frame();
        f.setSize(650, 750);
        f.setLocationRelativeTo(null);
        f.setVisible(true);

    }
}
