/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lab2sdindexservice;

import java.util.Comparator;

/**
 *
 * @author Seba
 */

//Clase para crear el comparador por frecuencia para los articulos
class FrecuenciaComparator implements Comparator {
    public int compare(Object o1, Object o2) {
        Articulo u1 = (Articulo) o1;
        Articulo u2 = (Articulo) o2;
        return u2.getFrecuencia()- u1.getFrecuencia();
    }
    public boolean equals(Object o) {
        return this == o;
    }
}
