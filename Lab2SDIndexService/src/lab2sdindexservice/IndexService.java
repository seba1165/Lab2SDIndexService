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
            String linea1 = br.readLine();
            LineaParticiones = linea1.split(" ");
            canTpart = Integer.parseInt(LineaParticiones[1]);

            String linea2 = br.readLine();
            LineaCantResultados = linea2.split(" ");
            cantResult = Integer.parseInt(LineaCantResultados[1]);
            
            //Validacion de parametros del config
            //Si los parametros son menores a 1, el caching service no corre
            if(canTpart<1 || cantResult<1){
                System.out.println("Ingrese los parametros de forma correcta");
            }else{
                fr.close();
                

                System.out.println("Inicializando CachingService... ");

                try {
                    //Socket para el servidor en el puerto 5000
                    acceptSocket = new ServerSocket(5000);
                    System.out.print("Server is running...");
                    System.out.println("\t[OK]\n");
                    int idSession = 0;
                    while (true) {
                        Socket connectionSocket;
                         //Socket listo para recibir
                        connectionSocket = acceptSocket.accept();
                        System.out.println("Nueva conexión entrante: "+connectionSocket);
                        (new Thread (new HiloIndexService(connectionSocket, idSession, db, canTpart, cantResult))).start();
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
