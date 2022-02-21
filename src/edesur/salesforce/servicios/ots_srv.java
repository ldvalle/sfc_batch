package edesur.salesforce.servicios;

import edesur.salesforce.dao.ost_dao;
import edesur.salesforce.entidades.ots_dto;
import java.util.Collection;
import java.util.Vector;
import java.util.Date;
import java.text.SimpleDateFormat;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class ots_srv {
    private static Writer outOT=null;
    private static String sPathGenera;
    private static String sPathCopia;
    private static String sArchOT;
    private static String sModoCorrida;
    private static Date dFechaDesde;
    private static Date dFechaHasta;

    public boolean ProcesaOTs(String sModoCorrida, String sOS, String ambiente, Date dFechaDesde, Date dFechaHasta){
        ost_dao miDAO = new ost_dao();

        //Abre archivos
        if(! AbreArchivos( sOS, ambiente, dFechaHasta)){
            System.out.println("No se pudo abrir el archivo. Proceso Abortado");
            System.exit(1);
        }

        //carga ots en el dao
        if(! miDAO.ProcesaOT(sModoCorrida, ambiente, dFechaDesde, dFechaHasta)){
            System.out.println("Fallo el DAO. Proceso Abortado");
            System.exit(1);
        }

        CierraArchivos();

        if(!MoverArchivo()) {
            System.out.println("No se pudo mover el archivo.");
        }

        return true;
    }

    private Boolean AbreArchivos( String sOS, String ambiente, Date dFechaHasta) {
        String sLinea="";
        ost_dao miDao = new ost_dao();
        String sClave = "";
        String sArchivoOT="";
        String sFilePathOT="";

        Date dFechaHoy = new Date();

        SimpleDateFormat fechaF = new SimpleDateFormat("yyyyMMdd");
        //String sFechaFMT=fechaF.format(dFechaHoy);
        String sFechaFMT=fechaF.format(dFechaHasta);


        sClave = "SALESF";
        sPathGenera=miDao.getRutaArchivo(sClave, ambiente);
        sClave = "SALEFC";
        sPathCopia=miDao.getRutaArchivo(sClave, ambiente);

        if(sOS.equals("DOS")){
            //Dell
            //sPathGenera="C:\\Users\\edesur\\Documents\\data_in\\";
            //sPathCopia="C:\\Users\\edesur\\Documents\\data_out\\";
            //Comodore
            sPathGenera="C:\\Users\\lukre\\Documents\\data_in\\";
            sPathCopia="C:\\Users\\lukre\\Documents\\data_out\\";

        }else{
            sPathGenera="/home/ldvalle/noti_in/";
            sPathCopia="/home/ldvalle/noti_out/";
        }

        sArchivoOT= String.format("enel_care_workorder_t1_%s.csv", sFechaFMT);
        sFilePathOT=sPathGenera.trim() + sArchivoOT.trim();

        sArchOT=sArchivoOT;

        try {
            outOT = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(sFilePathOT), StandardCharsets.UTF_8));
            sLinea = "\"Point Of Delivery\"";
            sLinea += "\"Cuenta\";";
            sLinea += "\"External ID\";";
            sLinea += "\"Order Number\";";
            sLinea += "\"Rol\";";
            sLinea += "\"Asunto\";";
            sLinea += "\"Descripcion\";";
            sLinea += "\"Estado\";";
            sLinea += "\"Fecha Estado\";";
            sLinea += "\"Codigo ISO\";";
            sLinea += "\r\n";

            try {
                outOT.write(sLinea);
            }catch(Exception e) {
                e.printStackTrace();
            }
        }catch(Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    void CierraArchivos() {
        try {
            outOT.close();
        }catch(Exception 	e) {
            e.printStackTrace();
        }
    }

    private Boolean MoverArchivo() {
        String sOriCnr = sPathGenera.trim() + sArchOT.trim();
        String sDestiCnr = sPathCopia.trim() + sArchOT.trim();

        Path pOriCnr = FileSystems.getDefault().getPath(sOriCnr);
        Path pDestiCnr = FileSystems.getDefault().getPath(sDestiCnr);

        try {
            Files.move(pOriCnr, pDestiCnr, StandardCopyOption.REPLACE_EXISTING);
        }catch(Exception e) {
            e.printStackTrace();
        }

        return true;
    }

    public Boolean GenerarPlano(ots_dto reg){
        String sLinea = "";
        String sAux="";
        int iLargo;

System.out.println("Mensaje " + reg.nro_mensaje);

        /* Point Of Delivery */
        sLinea = String.format("\"%dAR\";", reg.numero_cliente);

        /* Cuenta */
        sLinea += String.format("\"%dARG\";", reg.numero_cliente);

        /* External ID */
        sLinea += String.format("\"%sWOARG\";", reg.external_id.trim());

        /* Order Number */
        sLinea += String.format("\"%s\";", reg.sap_nro_ot.trim());

        /* Rol */
        if(reg.sRol == null){
            sLinea += "\"\";";
        }else{
            sLinea += String.format("\"%s\";", reg.sRol.trim());
        }

        /* Asunto */
        if(reg.descri_motivo.trim().equals("NORMALIZACIÓN"))
            reg.descri_motivo="NORMALIZACION";

        sLinea += String.format("\"%s\";", reg.descri_motivo.trim());

        /* Descripcion */

        iLargo =reg.sTexton.trim().length();
        if(iLargo > 300){
            sAux=reg.sTexton.trim().substring(0, 300);
        }else{
            sAux=reg.sTexton.trim();
        }
        if(sAux.equals("")){
            sLinea += "\"\";";
        }else{
            sLinea += String.format("\"%s\";", sAux.trim());
        }

        /* Estado */
        sLinea += String.format("\"%s\";", reg.desc_status.trim());

        /* Fecha Estado */
        sLinea += String.format("\"%s\";", reg.fecha_evento_fmt.trim());

        /* Codigo ISO */
        sLinea += "\"ARS\";";

        sLinea += "\r\n";

        try {
            outOT.write(sLinea);
        }catch(Exception e) {
            e.printStackTrace();
        }

        return true;
    }
}
