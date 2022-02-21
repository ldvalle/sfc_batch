package edesur.salesforce.dao;

import edesur.salesforce.conectBD.UConnection;
import edesur.salesforce.entidades.ots_dto;
import edesur.salesforce.entidades.periodos;
import edesur.salesforce.entidades.texton_dto;
import edesur.salesforce.servicios.ots_srv;
import javax.xml.transform.Result;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Locale;
import java.util.Vector;
import java.util.Date;

public class ost_dao {

    public boolean ProcesaOT(String modo, String ambiente, Date fechaDesde, Date fechaHasta){
        Connection con=null;
        PreparedStatement pstm=null;
        ResultSet rs=null;
        String SQLQUERY="";
        long lCantOT=0;
        ots_srv miSrv =new ots_srv();
        SimpleDateFormat fmtDesde = new SimpleDateFormat("yyyy-MM-dd 00:00:00");
        SimpleDateFormat fmtHasta = new SimpleDateFormat("yyyy-MM-dd 23:59:59");
        SimpleDateFormat fmtDTvacio = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String sFmtDesde=fmtDesde.format(fechaDesde);
        String sFmtHasta=fmtHasta.format(fechaHasta);
        Timestamp dtDesde = null;
        Timestamp dtHasta = null;

        try{
            dtDesde = Timestamp.valueOf(sFmtDesde);
            dtHasta = Timestamp.valueOf(sFmtHasta);
        }catch (Exception ex){
            ex.printStackTrace();
            System.exit(1);
        }

        try {
            con = UConnection.getConnection(ambiente);
            con.setAutoCommit(false);
            con.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);

            //Las OTs Normales
            System.out.println("Haciendo las nomales");
            if(modo.trim().equals("C")){
                //Carga Inicial
                pstm= con.prepareStatement(SEL_OTS_BASE_TEMPORAL);
                pstm.setTimestamp(1, dtDesde);
                pstm.setTimestamp(2, dtHasta);
                /*
                pstm.setDate(1, new java.sql.Date(fechaHasta.getTime())); //dateTime
                pstm.setDate(2, new java.sql.Date(fechaHasta.getTime())); //dateTime
                 */
            }else{
                //Delta Novedades
                pstm= con.prepareStatement(SEL_OTS_NOVE_TEMPORAL);
                pstm.setTimestamp(1, dtDesde);
                pstm.setTimestamp(2, dtHasta);
                /*
                pstm.setDate(1, new java.sql.Date(fechaDesde.getTime())); //dateTime
                pstm.setDate(2, new java.sql.Date(fechaHasta.getTime())); //dateTime
                */
                pstm.setDate(3, new java.sql.Date(fechaDesde.getTime())); //date
                pstm.setDate(4, new java.sql.Date(fechaHasta.getTime())); //date
            }

            pstm.executeUpdate();

            pstm=null;

            pstm=con.prepareStatement(SEL_TEMPORAL, ResultSet.TYPE_SCROLL_INSENSITIVE , ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
            pstm.setFetchSize(1);
            rs=pstm.executeQuery();

            while (rs.next()){
                ots_dto regOT = new ots_dto();

                regOT.nro_mensaje = rs.getLong(1);
                regOT.sap_nro_ot = rs.getString(2);
                regOT.tipo_orden = rs.getString(3);
                regOT.tema = rs.getString(4);
                regOT.trabajo = rs.getString(5);
                regOT.sap_status = rs.getString(6);
                regOT.desc_status = rs.getString(7);
                regOT.ot_cod_motivo = rs.getString(8);
                regOT.fecha_inicio = rs.getDate(9);
                regOT.fecha_status = rs.getDate(10);
                regOT.numero_cliente = rs.getLong(11);
                regOT.fecha_evento_fmt = rs.getString(12);
                regOT.external_id = rs.getString(13);
                regOT.sRol = rs.getString(14);

                //levantar descripcion motivo
                regOT.descri_motivo = getDescMotivo(regOT, con);
                //levantar el texton
                regOT.sTexton = getTexton(regOT.nro_mensaje, con);

                //Enviar la linea al servicio para que la procese
                if(!miSrv.GenerarPlano(regOT)){
                    System.exit(1);
                }

                lCantOT++;
            }
            pstm=null;

            //Las OTs Ficticias
            /*
            System.out.println("Haciendo las ficticias");
            if(modo.trim().equals("C")){
                pstm=con.prepareStatement(SEL_FICTICIAS_PENDIENTES, ResultSet.TYPE_SCROLL_INSENSITIVE , ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
            }else{
                pstm=con.prepareStatement(SEL_FICTICIAS_NOVE, ResultSet.TYPE_SCROLL_INSENSITIVE , ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
            }
            pstm.setTimestamp(1, dtDesde);
            pstm.setTimestamp(2, dtHasta);
            //pstm.setDate(1, new java.sql.Date(fechaDesde.getTime()));
            //pstm.setDate(2, new java.sql.Date(fechaHasta.getTime()));

            pstm.setFetchSize(1);
            rs=pstm.executeQuery();

            while (rs.next()){
                ots_dto regOT = new ots_dto();

                regOT.nro_mensaje = rs.getLong(1);
                regOT.sap_nro_ot = rs.getString(2);
                regOT.tipo_orden = rs.getString(3);
                regOT.tema = rs.getString(4);
                regOT.trabajo = rs.getString(5);
                regOT.sap_status = rs.getString(6);
                regOT.desc_status = rs.getString(7);
                regOT.ot_cod_motivo = rs.getString(8);
                regOT.fecha_inicio = rs.getDate(9);
                regOT.fecha_status = rs.getDate(10);
                regOT.numero_cliente = rs.getLong(11);
                regOT.fecha_evento_fmt = rs.getString(12);
                regOT.external_id = rs.getString(13);
                regOT.sRol = rs.getString(14);

                //levantar descripcion motivo
                regOT.descri_motivo = getDescMotivo(regOT, con);
                //levantar el texton
                regOT.sTexton = getTexton(regOT.nro_mensaje, con);

                //Enviar la linea al servicio para que la procese
                if(!miSrv.GenerarPlano(regOT)){
                    System.exit(1);
                }

                lCantOT++;
            }
            */
            pstm=null;

            System.out.println("OTs - Proceso Terminado OK.");
            System.out.println("OTs Procesadas " + lCantOT);

        }catch(SQLException ex){
            System.out.println("revento en la vuelta " + lCantOT );
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }finally{
            try{
                if(rs != null) rs.close();
                if(pstm != null) pstm.close();
            }catch(SQLException ex){
                ex.printStackTrace();
                throw new RuntimeException(ex);
            }
        }

        return true;
    }

    private String getDescMotivo(ots_dto regOT, Connection con) throws SQLException{
        String sDescMotivo="";
        String sTipoOT="";

        if(regOT.tipo_orden.trim().equals("GIS")){
            sDescMotivo="Motivo GISE " + regOT.ot_cod_motivo.trim();
        }else{
            if(regOT.tipo_orden.trim().equals("OT") || regOT.tipo_orden.trim().equals("OC")){
                sTipoOT="OTMOSO";
            }else if(regOT.tipo_orden.trim().equals("MAN")){
                sTipoOT="OTMOMA";
            }else if(regOT.tipo_orden.trim().equals("RET")){
                sTipoOT="OTMORE";
            }

            try(PreparedStatement stmt = con.prepareStatement(SEL_DESCRIP_MOTIVO)){
                stmt.setString(1, sTipoOT.trim());
                stmt.setString(2, regOT.ot_cod_motivo);
                try(ResultSet rs = stmt.executeQuery()){
                    if(rs.next()){
                        sDescMotivo=rs.getString(1);
                    }else{
                        sTipoOT="TRABAJ";

                        try(PreparedStatement stmt2 = con.prepareStatement(SEL_DESCRIP_MOTIVO)){
                            stmt2.setString(1, sTipoOT.trim());
                            stmt2.setString(2, regOT.ot_cod_motivo);
                            try(ResultSet rs2 = stmt2.executeQuery()){
                                if(rs2.next()){
                                    sDescMotivo=rs2.getString(1);
                                }
                            }

                        }
                    }
                }
            }

        }
        return sDescMotivo;
    }

    private String getTexton(long nro_mensaje, Connection con) throws SQLException{
        String sTexton="";

        try(PreparedStatement stmt = con.prepareStatement(SEL_TEXTON)){
            stmt.setLong(1,nro_mensaje);
            try(ResultSet rs = stmt.executeQuery()){
                while(rs.next()){
                    texton_dto rT=new texton_dto();
                    rT.setPagina(rs.getInt(1));
                    rT.setsTexto(rs.getString(2));
                    sTexton+= rT.getsTexto();
                }
            }
        }

        return sTexton;
    }


    public String getRutaArchivo(String clave, String ambiente) {
        String sRuta="";
        Connection conn = null;
        PreparedStatement stmt=null;
        ResultSet rs=null;

        try{
            conn = edesur.salesforce.conectBD.UConnection.getConnection(ambiente);
            stmt = conn.prepareStatement(SEL_RUTA_FILES);
            stmt.setString(1, clave);
            rs = stmt.executeQuery();
            if (rs.next()) {
                sRuta = rs.getString(1);
            }
        }catch (SQLException ex){
            ex.printStackTrace();
            System.exit(1);
        }

        return sRuta.trim();
    }

    public String getFechaActual(String ambiente) {
        String sFechaActual="";

        try(Connection conn = edesur.salesforce.conectBD.UConnection.getConnection(ambiente)){
            try(PreparedStatement stmt = conn.prepareStatement(FECHA_ACTUAL)){
                try(ResultSet rs = stmt.executeQuery()){
                    if(rs.next()){
                        sFechaActual=rs.getString(1);
                    }
                }
            }
        }catch (SQLException ex){
            ex.printStackTrace();
        }
        return sFechaActual;
    }

    public String getFechaActualFMT(String ambiente) {
        String sFechaActual="";

        try(Connection conn = edesur.salesforce.conectBD.UConnection.getConnection(ambiente)){
            try(PreparedStatement stmt = conn.prepareStatement(FECHA_ACTUAL_FMT)){
                try(ResultSet rs = stmt.executeQuery()){
                    if(rs.next()){
                        sFechaActual=rs.getString(1);
                    }
                }
            }
        }catch (SQLException ex){
            ex.printStackTrace();
        }
        return sFechaActual;
    }

    private static final String FECHA_ACTUAL = "SELECT TO_CHAR(TODAY, '%d/%m/%Y') FROM dual ";

    private static final String FECHA_ACTUAL_FMT = "SELECT TO_CHAR(TODAY, '%Y%m%d') FROM dual ";

    private static final String SEL_RUTA_FILES = "SELECT valor_alf "+
            "FROM tabla "+
            "WHERE nomtabla = 'PATH' "+
            "AND codigo = ? "+
            "AND sucursal = '0000' "+
            "AND fecha_activacion <= TODAY "+
            "AND ( fecha_desactivac >= TODAY OR fecha_desactivac IS NULL )";

    private static final String SEL_OTS_BASE_TEMPORAL = "SELECT o.mensaje_xnear, " +
            "o.numero_orden, " +
            "m.ot_nro_orden, " +
            "o.tipo_orden, " +
            "o.tema, " +
            "o.trabajo, " +
            "m.ot_status, " +
            "trim(s.descripcion) desc_status, " +
            "m.ot_motivo, " +
            "o.fecha_inicio, " +
            "date(m.ot_fecha_status) ot_fecha_status, " +
            "o.numero_cliente, " +
            "TO_CHAR(m.ot_fecha_status, '%Y-%m-%dT%H:%M:%S.000Z') fecha_sts_fmt, " +
            "LPAD(m.ot_nro_orden, 8, '0') external_id, " +
            "o.rol_usuario rol " +
            "from ot_mac m, orden o, ot_status s " +
            "WHERE o.mensaje_xnear = m.ot_mensaje_xnear " +
            "AND o.fecha_inicio BETWEEN ? AND ? " +
            "AND o.term_dir != 'SALESFORCE' " +
            "and s.codigo = m.ot_status " +
            "and s.fecha_activacion <= today " +
            "and (s.fecha_desactivac > today or s.fecha_desactivac is null) " +
            "INTO TEMP tempo1 WITH NO LOG ";

    private static final String SEL_OTS_NOVE_TEMPORAL = "SELECT o.mensaje_xnear, " +
            "o.numero_orden, " +
            "m.ot_nro_orden," +
            "o.tipo_orden, " +
            "o.tema, " +
            "o.trabajo, " +
            "m.ot_status, " +
            "trim(s.descripcion) desc_status, " +
            "m.ot_motivo, " +
            "o.fecha_inicio, " +
            "date(m.ot_fecha_status) ot_fecha_status, " +
            "o.numero_cliente, " +
            "TO_CHAR(m.ot_fecha_status, '%Y-%m-%dT%H:%M:%S.000Z') fecha_sts_fmt, " +
            "LPAD(m.ot_nro_orden, 8, '0') external_id, " +
            "o.rol_usuario rol " +
            "from ot_mac m, orden o, ot_status s " +
            "where m.ot_fecha_status between ? and ? " +
            "AND o.mensaje_xnear = m.ot_mensaje_xnear " +
            "AND o.term_dir != 'SALESFORCE' " +
            "and s.codigo = m.ot_status " +
            "and s.fecha_activacion <= today " +
            "and (s.fecha_desactivac > today or s.fecha_desactivac is null) " +
            "UNION " +
            "SELECT o.mensaje_xnear, " +
            "o.numero_orden, " +
            "m.otf_nro_orden ot_nro_orden, " +
            "o.tipo_orden, " +
            "o.tema, " +
            "o.trabajo, " +
            "case " +
            "   when m.estado = 'T' then 'REAL' " +
            "   else 'NORE' " +
            "end ot_status, " +
            "case " +
            "   when m.estado = 'T' then 'REALIZADO' " +
            "   else 'NO REALIZADO' " +
            "end desc_status, " +
            "m.cod_motivo, " +
            "o.fecha_inicio, " +
            "CASE " +
            "   WHEN m.fecha_ejecucion IS NOT NULL THEN m.fecha_ejecucion " +
            "   ELSE m.fecha_ot_final " +
            "END ot_fecha_status, " +
            "o.numero_cliente, " +
            "CASE " +
            "   WHEN m.fecha_ejecucion IS NOT NULL THEN TO_CHAR(m.fecha_ejecucion, '%Y-%m-%dT') || TO_CHAR(m.otf_hora_final, '%H:%M:%S.000Z') " +
            "   ELSE TO_CHAR(m.fecha_ot_final, '%Y-%m-%dT%H:%M:%S.000Z') " +
            "END fecha_sts_fmt, " +
            "LPAD(m.otf_nro_orden, 8, '0') external_id, " +
            "o.rol_usuario rol " +
            "FROM ot_final m, orden o " +
            "WHERE m.fecha_ot_final BETWEEN ? and ? " +
            "AND o.mensaje_xnear = m.mensaje_xnear " +
            "AND o.term_dir != 'SALESFORCE' " +
            "INTO TEMP tempo1 WITH NO LOG ";

    private static final String SEL_TEMPORAL = "SELECT t1.mensaje_xnear, " +
            "t1.ot_nro_orden, " +
            "t1.tipo_orden, " +
            "t1.tema, " +
            "t1.trabajo, " +
            "t1.ot_status, " +
            "t1.desc_status, " +
            "t1.ot_motivo, " +
            "t1.fecha_inicio, " +
            "t1.ot_fecha_status, " +
            "t1.numero_cliente, " +
            "t1.fecha_sts_fmt, " +
            "t1.external_id, " +
            "t1.rol " +
            "FROM tempo1 t1 " +
            "ORDER BY 1  ";

    private static final String SEL_FICTICIAS_PENDIENTES = "SELECT o.mensaje_xnear, " +
            "s.omp_nro_orden, " +
            "o.tipo_orden, " +
            "o.tema, " +
            "o.trabajo, " +
            "s.omp_status,  " +
            "NVL(s.omp_desc_status, s.omp_status), " +
            "m.ot_motivo, " +
            "o.fecha_inicio, " +
            "NVL(s.omp_fecha_status, s.omp_fecha_ejecut), " +
            "o.numero_cliente, " +
            "TO_CHAR(NVL(s.omp_fecha_status, s.omp_fecha_ejecut), '%Y-%m-%dT') || TO_CHAR(NVL(s.omp_hora_status, s.omp_hora_final), '%H:%M:%S.000Z') fecha_sts_fmt, " +
            "LPAD(to_number(s.omp_nro_orden[3,12]), 8, '0') external_id, " +
            "o.rol_usuario " +
            "FROM orden o, ot_mac m, ot_mac_pend s " +
            "WHERE o.fecha_inicio BETWEEN ? AND ? " +
            "AND o.term_dir != 'SALESFORCE' " +
            "AND o.numero_cliente > 0 " +
            "AND o.mensaje_xnear = m.ot_mensaje_xnear " +
            "AND m.ot_nro_orden = s.omp_nro_orden[5,12] ";

    private static final String SEL_FICTICIAS_NOVE = "SELECT o.mensaje_xnear, " +
            "s.omp_nro_orden, " +
            "o.tipo_orden, " +
            "o.tema,  " +
            "o.trabajo, " +
            "s.omp_status, " +
            "NVL(s.omp_desc_status, s.omp_status), " +
            "m.ot_motivo, " +
            "o.fecha_inicio, " +
            "s.omp_fecha_status, " +
            "o.numero_cliente, " +
            "TO_CHAR(s.omp_fecha_status, '%Y-%m-%dT') || NVL(TO_CHAR(s.omp_hora_status, '%H:%M:%S.000Z'), '00:00:00.000Z') fecha_sts_fmt, " +
            "LPAD(to_number(s.omp_nro_orden[3,12]), 8, '0') external_id, " +
            "o.rol_usuario " +
            "FROM orden o, ot_mac m, ot_mac_pend s " +
            "WHERE o.term_dir != 'SALESFORCE' " +
            "AND o.numero_cliente > 0 " +
            "AND o.mensaje_xnear = m.ot_mensaje_xnear " +
            "AND m.ot_nro_orden = s.omp_nro_orden[5,12] " +
            "AND m.ot_fecha_status BETWEEN ? AND ? ";

    private static final String SEL_DESCRIP_MOTIVO = "SELECT TRIM(descripcion) FROM tabla " +
            "WHERE nomtabla = ? " +
            "AND sucursal = '0000' " +
            "AND codigo = ? " +
            "AND fecha_activacion <= TODAY " +
            "AND (fecha_desactivac > TODAY OR fecha_desactivac IS NULL) ";

    private static final String SEL_TEXTON = "SELECT pagina, texton FROM xnear2:pagina " +
            "WHERE mensaje = ? " +
            "AND servidor = 1 " +
            "AND pagina <= 5 " +
            "ORDER BY pagina ";
}
