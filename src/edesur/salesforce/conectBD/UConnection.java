package edesur.salesforce.conectBD;

import java.sql.DriverManager;
import java.sql.Connection;
import java.util.ResourceBundle;
import edesur.salesforce.conectBD.Kripton;

public class UConnection {
	private static Connection con = null;
	private static String amb=null;
	private static String miKey="q1w2e3r4t5y6";

	public static Connection getConnection(String ambiente){
		ResourceBundle rb =null;
		String driver="";
		String url= "";
		String usr="";
		String pwd="";
		amb=ambiente;

		try{
			if(con == null ){
				Runtime.getRuntime().addShutdownHook(new MiShDwnHook());
				rb = ResourceBundle.getBundle("jdbc");
				driver = rb.getString("driver");
				if(ambiente.trim().equals("PROD")){
					url = rb.getString("url_PROD");
					//usr = Kripton.Desencriptar2(rb.getString("usr_PROD"), miKey);
					//pwd = Kripton.Desencriptar2(rb.getString("pwd_PROD"), miKey);

					usr = rb.getString("usr_PROD");
					pwd = rb.getString("pwd_PROD");
				}else{
					url = rb.getString("url_TEST");
					//usr = Kripton.Desencriptar2(rb.getString("usr_TEST"), miKey);
					//pwd = Kripton.Desencriptar2(rb.getString("pwd_TEST"), miKey);

					usr = rb.getString("usr_TEST");
					pwd = rb.getString("pwd_TEST");
				}
				Class.forName(driver);
				con = DriverManager.getConnection(url, usr, pwd);
			}
			return con;

		}catch(Exception ex){
			ex.printStackTrace();
			throw new RuntimeException("Error al crear conexión", ex);
		}
	}

	public static void nullConnection() {
		try {
			con=null;
			//con.close();
		}catch(Exception ex){
			ex.printStackTrace();
			throw new RuntimeException("Error al cerrar conexión " + ex.getMessage(), ex);
		}
	}

	static class MiShDwnHook extends Thread{
		public void run(){
			try{
				Connection con = UConnection.getConnection(amb);
				con.close();
			}catch(Exception ex){
				ex.printStackTrace();
				throw new RuntimeException(ex);
			}
		}
	}
}
