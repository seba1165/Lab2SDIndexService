/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lab2sdindexservice;

import java.util.Scanner;

/**
 *
 * @author Seba
 */


public class FrontService {
    //Front Service envia consultas al Index Service
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Ingrese las palabras de la busqueda: ");
        String query = scanner.nextLine();
        
        for (int i = 0; i < 1; i++) {
            //String numero_query = Integer.toString(i);
            //String query2  = query + numero_query;
            (new Thread (new HiloFrontService(i, query))).start();   
        }
    }

}