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
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import java.util.ArrayList;
import java.util.Collections;
import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.logging.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
public class HiloIndexService implements Runnable {
    //Atributos para conexion
    private Socket socket;
    private DataOutputStream outToClient;
    BufferedReader inFromClient;
    String fromClient;
    String processedData;
    //Id del hilo 
    private int idSession;
    //Atributos para el manejo del cache
    int particiones;
    int cantResultados;
   
    //String para mensaje enviado por el cliente;
    String request;
    DB db;
    
    public HiloIndexService(Socket socket, int id, DB db, int particiones, int cantResultados) throws IOException {
        this.socket = socket;
        this.idSession = id;
        this.db = db;
        this.particiones = particiones;
        this.cantResultados = cantResultados;
    }

    public void desconnectar() {
        try {
            socket.close();
        } catch (IOException ex) {
            Logger.getLogger(HiloIndexService.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void run() {
        try {
            //Recibe consultas del Front Service u ordenes de ingresar informacion al cache desde el Index Service
            outToClient = new DataOutputStream(socket.getOutputStream());
            inFromClient = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            //dis = new DataInputStream(socket.getInputStream());
            fromClient =inFromClient.readLine();
            request = fromClient;
            
            //System.out.println("Servidor "+ idSession);
            //Los mensajes recibidos tienen el formato REST
            System.out.println(request);
            String[] tokens = request.split(" ");
            String parametros = tokens[1];
            int espacios = tokens.length;
            String http_method = tokens[0];

            String[] tokens_parametros = parametros.split("/");
            String resource = tokens_parametros.length > 1 ? tokens_parametros[1] : "";

            String id = tokens_parametros.length > 2 ? tokens_parametros[2] : "";
            int cantidadQuerys=0;
            System.out.println("Partes restantes del query: "+(tokens.length-2));
            
            if (tokens.length-2>0){
                for (int i = 0; i < tokens.length-2; i++) {
                    id += " "+tokens[i+2];
                }
            }
            System.out.println("El query completo es "+id);
            
            
            for (int i = 0; i < tokens.length; i++) {
                System.out.println(tokens[i]);
            }
            
            for (int i = 0; i < tokens_parametros.length; i++) {
                System.out.println(tokens_parametros[i]);
            }
            String meta_data = tokens.length > 2 ? tokens[2] : "";
            
            
            System.out.println("\nConsulta: " + request);
            System.out.println("HTTP METHOD: " + http_method);
            System.out.println("Resource: " + resource);
            System.out.println("ID:       " + id);
            System.out.println("META DATA:    " + meta_data);
            
            //System.out.println("La consulta se deberia encontrar en la posicion "+posicion_consulta);
            
            String[] palabras = id.split(" ");
            DBObject querys[] = new DBObject[palabras.length];
            //System.out.println(palabras.length);
            Palabra Indice[] = new Palabra[palabras.length];
            //System.out.println(Indice.length);
            
            ArrayList Articulos;
            for (int i = 0; i < Indice.length; i++) {
                String palabraAux = palabras[i];
                ArrayList arreglo = new ArrayList();
                Indice[i] = new Palabra(palabraAux, arreglo);
            }
            
            String title = "" ;
            String frecuencia = "";
            for (int i = 0; i < querys.length; i++) {
                BasicDBObject query = new BasicDBObject();
                //System.out.println(palabras[i]);
                query.put("key", palabras[i]);
                int posicion = funcion_hash(palabras[i], particiones);
                //System.out.println("posicion " + posicion);
                String coleccionIndice = "IndiceInvertido"+posicion;
                DBCollection IndiceInvertido =  db.getCollection(coleccionIndice);
                querys[i] = IndiceInvertido.findOne(query);               
                int auxTitle=0;
                int auxFrecuencia=0;
                if (querys[i]!=null){
                    Articulos = new ArrayList(); 
                    String delimitadores = "[:,\"{}\\[\\]]+"; 
                    String[] palabrasSeparadas = querys[i].get("articulos").toString().split(delimitadores);
                    for (int j = 0; j < palabrasSeparadas.length-1; j++) {
                        if (palabrasSeparadas[j].length()!=0 && !palabrasSeparadas[j].equals(" ")){
                            //System.out.println(palabrasSeparadas[j]);
                            if (auxTitle==1) {
                                title = palabrasSeparadas[j];
                                auxTitle=0;
                            }
                            if (auxFrecuencia==1) {
                                frecuencia = palabrasSeparadas[j];
                                Articulo articulo = new Articulo(title, frecuencia);
                                Indice[i].Articulos.add(articulo);
                                auxFrecuencia = 0;
                            }
                            if (palabrasSeparadas[j].equals("title")) {
                                auxTitle+=1;
                            }
                            if (palabrasSeparadas[j].equals("frecuencia")){
                                auxFrecuencia+=1;
                            }
                        }
                    }
                    Collections.sort(Indice[i].Articulos, new FrecuenciaComparator()); 
                }
            }
            
            for (int i = 0; i < Indice.length; i++) {
            //System.out.println(Indice[i].palabra);
                for (int j = 0; j < Indice[i].Articulos.size(); j++) {
                    Articulo aux = (Articulo)Indice[i].Articulos.get(j);
                    //System.out.println(aux.title);
                    //System.out.println(aux.frecuencia);
                }
            }
        
            ArrayList resultado = calculaMejores(Indice, db, particiones,cantResultados);
            ArrayList<String> resultado2 = new ArrayList<String>();
            Collections.sort(resultado, new ScoreComparator()); 
            for (int i = 0; i < resultado.size(); i++) {
                Articulo articulo = (Articulo)resultado.get(i);
                //System.out.println(articulo.title);
                if (resultado2.size()==0) {
                    resultado2.add(articulo.title);
                }else{
                    if (resultado2.contains(articulo.title)) {
                        //
                    }else{
                        resultado2.add(articulo.title);
                    }
                }
                //System.out.println(articulo.score);
            }
            if (resultado2.size()<cantResultados) {
                cantResultados = resultado2.size();
            }
            
            System.out.println("===========");
            System.out.println(cantResultados+" Mejores");
            System.out.println("===========");
            JSONArray objResultados = new JSONArray();
            for (int i = 0; i < cantResultados; i++) {
                JSONObject objResultado = new JSONObject();
                int posicion = funcion_hash(resultado2.get(i), particiones);
                String coleccionIndice = "Wikipedia"+posicion;
                DBCollection Wikipedia =  db.getCollection(coleccionIndice);
                BasicDBObject query = new BasicDBObject();
                //System.out.println(palabras[i]);
                query.put("title", resultado2.get(i));
                DBObject result = Wikipedia.findOne(query);
                if (result != null) {
                    String text = result.get("text").toString();
                    String split[] = text.split(" ");
                    String resumen = "";
                    int cantPalabras = split.length;
                    if (cantPalabras > 100){
                        cantPalabras = 100;
                    }
                    for (int j = 0; j < cantPalabras; j++) {
                        if (split[j].length()!=0 && !split[j].equals(" ")){
                            //System.out.println(split[j]);
                            resumen = resumen + split[j];
                            resumen = resumen + " ";
                        }else{
                            cantPalabras+=1;
                        }
                    }
                    objResultado.put("resumen", resumen);
                    objResultado.put("title", result.get("title"));
                    objResultado.put("_id", result.get("_id"));
                    //System.out.println(objResultado);
                    objResultados.add(objResultado);
                }
            }
            
            System.out.println(objResultados);
            
            outToClient.writeBytes(objResultados.toJSONString());
            

            
        } catch (IOException ex) {
            Logger.getLogger(HiloIndexService.class.getName()).log(Level.SEVERE, null, ex);
        }
        desconnectar();
    }
    //Funcion para calcular en que particion del cache se guarda la informacion que ha llegado desde el index Service
    static int funcion_hash(String x, int particiones) {
        char ch[];
        ch = x.toCharArray();
        int xlength = x.length();
        int i, sum;
        for (sum=0, i=0; i < x.length(); i++)
            sum += ch[i];
        return sum % particiones;
    }

    private static ArrayList calculaMejores(Palabra[] Indice, DB db, int particiones, int cantResultados) {
        ArrayList resultado =  new ArrayList();
        int cont = 0;
        int menor = -1;
        for (int i = 0; i < Indice.length; i++) {
            if (menor == -1) {
                menor = Indice[i].Articulos.size();
            }
            if (Indice[i].Articulos.size()< menor && Indice[i].Articulos.size()!=0) {
                    menor = Indice[i].Articulos.size();
            }
        }
        
        for (int i = 0; i < Indice.length; i++) {
            ArrayList arreglo = Indice[i].Articulos;
            for (int j = 0; j < menor; j++) {
                if (arreglo.size()!=0) {
                    Articulo articuloArreglo = (Articulo)arreglo.get(j);
                    String title = articuloArreglo.getTitle();
                    int frecuencia = articuloArreglo.getFrecuencia();
                    //System.out.println(title+" "+frecuencia);
                    //System.out.println(frecuencia+" para "+ Indice[i].palabra + " en "+ title );
                    int score=frecuencia;
                    for (int k = 0; k < Indice.length; k++) {
                        if (i!=k) {
                            score += buscaEnArticulo(Indice[k].palabra, title, db, particiones); 
                        }
                    }
                    //System.out.println("El puntaje acumulado para "+title+ " es "+score);
                    Articulo articuloEnArregloResultado = new Articulo(title, score);
                    resultado.add(articuloEnArregloResultado);
                    
                }
            }
            //System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            
        }
        
        return resultado;
    }

    private static int buscaEnArticulo(String palabra, String title,  DB db, int particiones) {
        BasicDBObject palabraIndice = new BasicDBObject();
        palabraIndice.put("key", palabra);
        int posicion = funcion_hash(palabra, particiones);
        //System.out.println("posicion " + posicion);
        //System.out.println(palabra);
        String coleccionIndice = "IndiceInvertido"+posicion;
        DBCollection IndiceInvertido =  db.getCollection(coleccionIndice);
        DBObject palabraEnIndice = IndiceInvertido.findOne(palabraIndice);
        
        if (palabraEnIndice != null) {
            ///System.out.println(palabraEnIndice.get("articulos"));
            String split[] = palabraEnIndice.get("articulos").toString().split("\""+title+"\" , \"frecuencia\" : ");
            //System.out.println(split.length);
            
            if (split.length==2) {
                //System.out.println(split[1]);
                String split2[] = split[1].split("}");
                int frecuencia = Integer.parseInt(split2[0]);
                //System.out.println(frecuencia + " para "+ palabra + " en "+title);
                return frecuencia;
            }else{
                return 0;
            }
        } else {
            return 0;
        }
    }
     
}
