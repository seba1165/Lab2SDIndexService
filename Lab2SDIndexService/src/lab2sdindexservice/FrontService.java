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
import java.util.*;


public class FrontService {
    //Front Service envia consultas al Caching Service
    public static void main(String[] args) {
        String query = "arroz leche";
        int numero;
        
        for (int i = 0; i < 1; i++) {
            //String numero_query = Integer.toString(i);
            //String query2  = query + numero_query;
            (new Thread (new HiloFrontService(i, query))).start();   
        }
    }

}