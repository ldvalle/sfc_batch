package edesur.salesforce.ppal;

import edesur.salesforce.servicios.ots_srv;
import java.util.Date;
import java.util.Locale;
import java.text.SimpleDateFormat;

public class sfc_ots {
    static private String sModoCorrida; //Delta - Carga Inicial
    static private String sSO;  //DOS - UNIX
    static private String ambiente; //PROD - TEST
    static private Date dFechaDesde;
    static private Date dFechaHasta;

    public static void main(String[] args) {

        ots_srv miSrv = new ots_srv();
        SimpleDateFormat fechaF = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

        Date fechaInicio = new Date();

        if(!ValidaArgumentos(args)) {
            System.exit(1);
        }
        if(sSO.equals("DOS")) {
            Locale.setDefault(Locale.Category.FORMAT, java.util.Locale.US);
        }

        System.out.println("Procesando OTs ...");

        if(!miSrv.ProcesaOTs(sModoCorrida, sSO, ambiente, dFechaDesde, dFechaHasta)) {
            System.out.println("Fallo el proceso");
            System.exit(1);
        }

        System.out.println("Termino OK");

        Date fechaFin = new Date();

        System.out.println("Inicio: " + fechaF.format(fechaInicio));
        System.out.println("Fin:    " + fechaF.format(fechaFin));

    }

    static private Boolean ValidaArgumentos(String[] args) {
        SimpleDateFormat fechaF = new SimpleDateFormat("dd/MM/yyyy");

        if(args.length != 5) {
            System.out.println("Argumentos Invalidos");
            System.out.println("Modo Corrida: C = Carga Inicial; D = Delta");
            System.out.println("Sistema Operativo: DOS; UNIX");
            System.out.println("Ambiente: PROD; TEST");
            System.out.println("Fecha Desde: dd/mm/aaaa");
            System.out.println("Fecha Hasta: dd/mm/aaaa");

            return false;
        }

        sModoCorrida = args[0];
        sSO = args[1];
        ambiente = args[2];
        try {
            dFechaDesde = fechaF.parse(args[3]);
            dFechaHasta = fechaF.parse(args[4]);
        }catch (Exception ex){
            ex.printStackTrace();
            System.exit(1);
        }
        return true;
    }

}
