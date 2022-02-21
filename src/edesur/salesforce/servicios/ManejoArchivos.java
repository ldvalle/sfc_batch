package edesur.salesforce.servicios;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;

public class ManejoArchivos {

    //Abrir un Archivo Escritura
    public Writer AbreArchivoEscritura(String sPathNombre){
        Writer pfOut=null;
        try {
            pfOut = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(sPathNombre), "UTF-8"));
        }catch(Exception e) {
            e.printStackTrace();
        }
        return pfOut;
    }
    // Abrir un archivo de Lectura
    public BufferedReader AbreArchivoLectura(String sPathNombre){
        BufferedReader pfIn=null;

        try{
            pfIn=new BufferedReader(new InputStreamReader(new FileInputStream(sPathNombre)));
        }catch (Exception e){
            e.printStackTrace();
        }
        return pfIn;
    }

    //Escribir una linea en el archivo
    public boolean InsertaLinea(Writer pfOut, String sLinea){

        try {
            pfOut.write(sLinea);
        }catch(Exception e) {
            e.printStackTrace();
        }

        return true;
    }

    //Cerrar un archivo
    public boolean CierraArchivo(Writer pfOut){
        try {
            pfOut.close();
        }catch(Exception 	e) {
            e.printStackTrace();
        }

        return true;
    }

    //Mover un archivo
    public boolean MoverArchivo(Path pOrigen, Path pDestino){

        try {
            Files.move(pOrigen, pDestino, StandardCopyOption.REPLACE_EXISTING);
        }catch(Exception e) {
            e.printStackTrace();
        }

        return true;
    }

}
