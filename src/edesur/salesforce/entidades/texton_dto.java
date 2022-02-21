package edesur.salesforce.entidades;

public class texton_dto {
    private int  pagina;
    private String   sTexto;

    public int getPagina() {
        return pagina;
    }

    public void setPagina(int pagina) {
        this.pagina = pagina;
    }

    public String getsTexto() {
        return ParseaTexton(sTexto);
    }

    public void setsTexto(String sTexto) {
        this.sTexto = sTexto;
    }

    private String ParseaTexton(String sTexto){
        String sNvaCadena="";

        for(int i=0; i< sTexto.length(); i++){
            int iVal=sTexto.charAt(i);
            if(iVal<32 || iVal>126){
                sNvaCadena+= " ";
            }else{
                sNvaCadena+= (char) iVal;
            }
        }

        sNvaCadena=sNvaCadena.replace('þ', ' ');
        sNvaCadena=sNvaCadena.replace(',', ' ');
        sNvaCadena=sNvaCadena.replace('\\', ' ');
        sNvaCadena=sNvaCadena.replace('\r', ' ');
        sNvaCadena=sNvaCadena.replace('\n', ' ');

        return sNvaCadena;
    }


}
