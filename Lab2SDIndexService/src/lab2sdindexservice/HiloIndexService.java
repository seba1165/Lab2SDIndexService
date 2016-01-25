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
    String ipCaching;
    String puertoCaching;
    //Id del hilo 
    private int idSession;
    //Atributos para el manejo del cache
    int particiones;
    int cantResultados;
    private DataOutputStream salidaCaching;
    //String para mensaje enviado por el cliente;
    String request;
    //Base de datos
    DB db;
    
    public HiloIndexService(Socket socket, String ipCaching, String puertoCaching, int id, DB db, int particiones, int cantResultados) throws IOException {
        this.socket = socket;
        this.idSession = id;
        this.db = db;
        this.particiones = particiones;
        this.cantResultados = cantResultados;
        this.ipCaching = ipCaching;
        this.puertoCaching = puertoCaching;
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
            //Recibe consultas del Front Service.
            outToClient = new DataOutputStream(socket.getOutputStream());
            inFromClient = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            //dis = new DataInputStream(socket.getInputStream());
            fromClient =inFromClient.readLine();
            request = fromClient;
            
            //System.out.println("Servidor "+ idSession);
            //Los mensajes recibidos tienen el formato REST
            System.out.println(request);
            String[] tokens = request.toLowerCase().split(" ");
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
//            for (int i = 0; i < tokens.length; i++) {
//                System.out.println(tokens[i]);
//            }
//            for (int i = 0; i < tokens_parametros.length; i++) {
//                System.out.println(tokens_parametros[i]);
//            }
            String meta_data = tokens.length > 2 ? tokens[2] : "";
            
            
            System.out.println("\nConsulta: " + request);
            System.out.println("HTTP METHOD: " + http_method);
            System.out.println("Resource: " + resource);
            System.out.println("ID:       " + id);
            System.out.println("META DATA:    " + meta_data);
            
            //System.out.println("La consulta se deberia encontrar en la posicion "+posicion_consulta);
            
            //Palabras de la consulta separadas
            String[] palabras = id.split(" ");
            //Cantidad de querys igual a la cantidad de palabras a buscar en el indice invertido
            DBObject querys[] = new DBObject[palabras.length];
            //System.out.println(palabras.length);
            Palabra Indice[] = new Palabra[palabras.length];
            //System.out.println(Indice.length);
            ArrayList Articulos;
            for (int i = 0; i < Indice.length; i++) {
                String palabraAux = palabras[i];
                ArrayList arreglo = new ArrayList();
                //Se crea la palabra junto al arreglo vacio de articulos correspondiente a dicha palabra
                Indice[i] = new Palabra(palabraAux, arreglo);
            }
            
            String title = "" ;
            String frecuencia = "";
            //Para cada query
            for (int i = 0; i < querys.length; i++) {
                BasicDBObject query = new BasicDBObject();
                //System.out.println(palabras[i]);
                //Se crea el query para buscar la palabra
                query.put("key", palabras[i]);
                //Particion de la palabra en el indice invertido
                int posicion = funcion_hash(palabras[i], particiones);
                //System.out.println("posicion " + posicion);
                String coleccionIndice = "IndiceInvertido"+posicion;
                DBCollection IndiceInvertido =  db.getCollection(coleccionIndice);
                querys[i] = IndiceInvertido.findOne(query);               
                int auxTitle=0;
                int auxFrecuencia=0;
                //Si el query existe en la bd
                if (querys[i]!=null){
                    Articulos = new ArrayList(); 
                    String delimitadores = "[,\"{}\\[\\]]+"; 
                    //Se eliminan los caracteres inservibles de los articulos que contienen la palabra
                    String[] palabrasSeparadas = querys[i].get("articulos").toString().split(delimitadores);
                    for (int j = 0; j < palabrasSeparadas.length-1; j++) {
                        if (palabrasSeparadas[j].length()!=0 && !palabrasSeparadas[j].equals(" ")){
                            //System.out.println(palabrasSeparadas[j]);
                            //System.out.println(palabrasSeparadas[j]);
                            //Si antes la palabra era title, ahora se lee y guarda el titulo
                            if (auxTitle==2) {
                                title = palabrasSeparadas[j];
                                //System.out.println(title);
                                auxTitle=0;
                            }else if(auxTitle==1){
                                auxTitle+=1;
                            }
                            //Si antes la palabra era frecuencia, ahora se lee y guarda la frecuencia
                            if (auxFrecuencia==1) {
                                frecuencia = palabrasSeparadas[j];
                                //Se crea el articulo con los datos correspondientes
                                Articulo articulo = new Articulo(title, frecuencia);
                                //Se agrega el articulo al arreglo de articulos correspondiente a la palabra
                                Indice[i].Articulos.add(articulo);
                                auxFrecuencia = 0;
                            }
                            //Si se lee la palabra title, despues vendra : y despues el titulo
                            if (palabrasSeparadas[j].equals("title")) {
                                auxTitle+=1;
                            }
                            //Si se lee la palabra frecuencia, despues se lee la frecuencia
                            if (palabrasSeparadas[j].equals("frecuencia")){
                                auxFrecuencia+=1;
                            }
                        }
                    }
                    //Se ordena el arreglo de articulo por la cantidad de frecuencia
                    Collections.sort(Indice[i].Articulos, new FrecuenciaComparator()); 
                }
            }

            //Se calculan los mejores resultados
            ArrayList resultado = calculaMejores(Indice, db, particiones, cantResultados);
            ArrayList<String> resultado2 = new ArrayList<String>();
            //Se ordenan los resultados segun el score acumulado
            Collections.sort(resultado, new ScoreComparator()); 
            //Se eliminan los articulos repetidos
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
            
            //Si la cantidad de resultados obtenidos es menor a la cantidad solicitada en el txt de configuracion
            if (resultado2.size()<cantResultados) {
                //Se cambia la cantidad de resultados a mostrar por la menor cantidad
                cantResultados = resultado2.size();
            }
            //Se guarda el primero para mandarlo al cache
            //System.out.println(resultado2.get(i));
            int posicion = funcion_hash(resultado2.get(0), particiones);
            String coleccionIndicePr = "Wikipedia"+posicion;
            BasicDBObject queryPrimero = new BasicDBObject();
            queryPrimero.put("title", resultado2.get(0));
            DBCollection WikipediaPr =  db.getCollection(coleccionIndicePr);
            //Se busca el articulo
            DBObject resultPr = WikipediaPr.findOne(queryPrimero);
            String titlePrimero = resultado2.get(0);
            String textPrimero = resultPr.get("text").toString();
            //Se agregan los mejores resultados al JSON de respuesta
            System.out.println("===========");
            System.out.println(cantResultados+" Mejores");
            System.out.println("===========");
            JSONArray objResultados = new JSONArray();
            //Para la cantidad de respuestas correspondientes, se busca el articulo en la bd
            for (int i = 0; i < cantResultados; i++) {
                JSONObject objResultado = new JSONObject();
                //Posicion en la particion de la bd segun el titulo del articulo
                posicion = funcion_hash(resultado2.get(i), particiones);
                //System.out.println(posicion);
                String coleccionIndice = "Wikipedia"+posicion;
                DBCollection Wikipedia =  db.getCollection(coleccionIndice);
                BasicDBObject query = new BasicDBObject();
                //System.out.println(palabras[i]);
                query.put("title", resultado2.get(i));
                //System.out.println(resultado2.get(i));
                //Se busca el articulo
                DBObject result = Wikipedia.findOne(query);
                if (result != null) {
                    //Se obtiene el texto para poder sacar un breve resumen
                    String text = result.get("text").toString();
                    //Se separa el texto por palabras
                    String split[] = text.split(" ");
                    String resumen = "";
                    //Se muestran 100 palabras del texto del articulo
                    //Si el articulo tiene menos palabras, se muestran todas
                    int cantPalabras = split.length;
                    if (cantPalabras > 100){
                        cantPalabras = 100;
                    }
                    //Se crea el resumen para el articulo
                    for (int j = 0; j < cantPalabras; j++) {
                        if (split[j].length()!=0 && !split[j].equals(" ")){
                            //System.out.println(split[j]);
                            resumen = resumen + split[j];
                            resumen = resumen + " ";
                        }else{
                            cantPalabras+=1;
                        }
                    }
                    //Se crea y agrega el articulo al JSON que se enviará al Front Service
                    objResultado.put("resumen", resumen);
                    objResultado.put("title", result.get("title"));
                    System.out.println((i+1)+". "+result.get("title"));
                    objResultado.put("_id", result.get("_id"));
                    //System.out.println(objResultado);
                    objResultados.add(objResultado);
                }
            }
            if (cantResultados==0) {
                Socket socketCaching = new Socket(InetAddress.getByName(ipCaching), 8090);
                salidaCaching = new DataOutputStream(socketCaching.getOutputStream());
                salidaCaching.writeBytes("No Hay Resultados");
                outToClient.writeBytes("No hay resultados"); 
                socketCaching.close();
            }else{
                //Se envia el JSON con el mejor resultado al cache
                //System.out.println(titlePrimero);
                //System.out.println(textPrimero);
                    
                String rest = "POST /respuestas/"+id+" body="+objResultados.toString();
                System.out.println(rest);
                Socket socketCaching = new Socket(InetAddress.getByName(ipCaching), Integer.parseInt(puertoCaching));
                salidaCaching = new DataOutputStream(socketCaching.getOutputStream());
                salidaCaching.writeBytes(rest);
                //salidaCaching.writeBytes("POST /respuestas/hola body=<p>hola mundo</>");
                outToClient.writeBytes(objResultados.toJSONString());
                socketCaching.close();
            }
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

    //Metodo que calcula los mejores resultados para la busqueda realizada
    private static ArrayList calculaMejores(Palabra[] Indice, DB db, int particiones, int cantResultados) {
        ArrayList resultado =  new ArrayList();
        ArrayList<String> aux = new ArrayList<String>();
        int cont = 0;
        int menor = -1;
        //Primero se ve que palabra tiene menor cantidad de articulos
        for (int i = 0; i < Indice.length; i++) {
            if (menor == -1) {
                menor = Indice[i].Articulos.size();
            }
            if (Indice[i].Articulos.size()< menor && Indice[i].Articulos.size()!=0) {
                    menor = Indice[i].Articulos.size();
            }
        }
        if (menor>cantResultados) {
            menor = cantResultados;
        }
        //Procedimiento para ir revisando los primeros articulos de cada palabra
        //Los articulo estan ordenados por frecuencia de aparicion de dicha palabra
        //por lo que se asume que los primeros articulos de cada palabra seran mejores
        //candidatos a mejor resultado que los demas.
        int palabra=0;
        int articulo=0;
        while (resultado.size()<cantResultados) {            
            ArrayList arreglo = Indice[palabra].Articulos;
            if (arreglo.size()!=0) {
                //Se obtiene el titulo y frecuencia del articulo correspondiente del arreglo
                Articulo articuloArreglo = (Articulo)arreglo.get(articulo);
                String title = articuloArreglo.getTitle();
                int frecuencia = articuloArreglo.getFrecuencia();
                if (!aux.contains(title)){
                    int score=frecuencia;
                    //Se buscan las demas palabras en el mismo articulo y se suman estas frecuencias en caso de existir
                    for (int k = 0; k < Indice.length; k++) {
                        if (palabra!=k) {
                            score += buscaEnArticulo(Indice[k].palabra, title, db, particiones); 
                        }
                    }
                    //Se crea el articulo con el puntaje acumulado
                    Articulo articuloEnArregloResultado = new Articulo(title, score);
                    //Se agrega al arreglo de resultados
                    resultado.add(articuloEnArregloResultado);
                    aux.add(title);
                    //System.out.println(title+" "+score);
                }
            }
            
            palabra+=1;
            if (palabra==Indice.length) {
                palabra=0;
                articulo+=1;
            }
        }
        //System.out.println(resultado.size());
        return resultado;
    }

    //Método para buscar la frecuencia de una palabra en un artículo
    private static int buscaEnArticulo(String palabra, String title,  DB db, int particiones) {
        //Se crea el query para buscar la palabra en el indice invertido
        BasicDBObject palabraIndice = new BasicDBObject();
        palabraIndice.put("key", palabra);
        //Particion a la que deberia pertener la palabra en caso de existir
        int posicion = funcion_hash(palabra, particiones);
        String coleccionIndice = "IndiceInvertido"+posicion;
        DBCollection IndiceInvertido =  db.getCollection(coleccionIndice);
        DBObject palabraEnIndice = IndiceInvertido.findOne(palabraIndice);
        //Si la palabra existe en la bd
        if (palabraEnIndice != null) {
            ///System.out.println(palabraEnIndice.get("articulos"));
            String split[] = palabraEnIndice.get("articulos").toString().split("\""+title+"\" , \"frecuencia\" : ");
            //System.out.println(split.length);
            
            if (split.length==2) {
                //System.out.println(split[1]);
                String split2[] = split[1].split("}");
                //Se saca la frecuencia del string
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
