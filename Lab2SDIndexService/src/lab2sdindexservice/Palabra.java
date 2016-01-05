/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lab2sdindexservice;

import java.util.ArrayList;

/**
 *
 * @author Seba
 */
//Clase para instanciar las palabras junto a su arreglo de articulos
class Palabra {
    String palabra;
    ArrayList Articulos;

    Palabra(String palabraAux, ArrayList Articulos) {
        palabra = palabraAux;
        this.Articulos = Articulos;
    }
}
