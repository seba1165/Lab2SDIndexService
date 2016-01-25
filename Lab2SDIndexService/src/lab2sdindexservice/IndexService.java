/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lab2sdindexservice;

/**
 *
 * @author Seba
 */
import com.mongodb.DB;
import com.mongodb.MongoClient;
import java.io.*;
import java.net.*;
import java.util.logging.*;

public class IndexService {
    public static void main(String args[]) throws IOException {
        //Variables para el Caching Service
        ServerSocket acceptSocket;
        String [] LineaParticiones;
        String [] LineaCantResultados;
        String [] LineaIp;
        String [] LineaCachingPuerto;
        String [] LineaPuerto;
        String ipCaching;
        String puertoCaching;
        String puerto;
        int canTpart;
        int cantResult;
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB("LabSD");

        try{
            //Config tiene los parametros de configuracion del cache
            File archivo = new File("Config.txt");
 
            FileReader fr = new FileReader(archivo);

            BufferedReader br = new BufferedReader(fr);

            //Lineas del Config.txt
            //Linea 1 tiene la cantidad de particiones de la bd
            String linea1 = br.readLine();
            LineaParticiones = linea1.split(" ");
            canTpart = Integer.parseInt(LineaParticiones[1]);
            //Linea 2 tiene la cantidad de resultados que se desean mostrar como respuesta
            String linea2 = br.readLine();
            LineaCantResultados = linea2.split(" ");
            cantResult = Integer.parseInt(LineaCantResultados[1]);
            //Linea 3 obtiene la ip del Caching Service
            String linea3 = br.readLine();
            LineaIp = linea3.split(" ");
            ipCaching = LineaIp[1];
            //Linea 4 obtiene el puerto del Caching Service
            String linea4 = br.readLine();
            LineaCachingPuerto = linea4.split(" ");
            puertoCaching = LineaCachingPuerto[1];
            //Linea 5 obtiene el puerto para recibir del Front Service
            String linea5 = br.readLine();
            LineaPuerto = linea5.split(" ");
            puerto = LineaCachingPuerto[1];
            fr.close();
            //Validacion de parametros del config
            //Si los parametros son menores a 1, el caching service no corre
            if(canTpart<1 || cantResult<1){
                System.out.println("Ingrese los parametros de forma correcta");
            }else{
                System.out.println("Inicializando Index Service... ");
                try {
                    //Socket para Recibir mensajes del FS
                    acceptSocket = new ServerSocket(Integer.parseInt(puerto));
                    Socket socketCaching;
                    //Socket para enviar al caching
                    
                    System.out.print("Server is running...");
                    System.out.println("\t[OK]\n");
                    int idSession = 0;
                    while (true) {
                        Socket connectionSocket;
                         //Socket listo para recibir
                        connectionSocket = acceptSocket.accept();
                        System.out.println("Nueva conexión entrante: "+connectionSocket);
                        //Por cada conexion, se inicia un hilo
                        (new Thread (new HiloIndexService(connectionSocket, ipCaching, puertoCaching, idSession, db, canTpart, cantResult))).start();
                        idSession++;
                    }
                } catch (IOException ex) {
                    Logger.getLogger(IndexService.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }catch (Exception e){ //Catch de excepciones
            System.err.println("Ocurrio un error: " + e.getMessage());
        }
    }

}
