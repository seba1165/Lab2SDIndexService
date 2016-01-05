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
class Articulo implements Comparable<Articulo>{
    String title;
    int frecuencia;
    int score;

    Articulo(String title, String frecuencia) {
        this.title = title;
        String split[] = frecuencia.split(" ");
        this.frecuencia = Integer.parseInt(split[1]);
    }

    public Articulo(String title, int score) {
        this.title = title;
        this.score = score;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public int getFrecuencia() {
        return frecuencia;
    }

    public void setFrecuencia(int frecuencia) {
        this.frecuencia = frecuencia;
    }
    

    @Override
    public int compareTo(Articulo o) {
        Articulo otroUsuario = (Articulo)o;
        //podemos hacer esto porque String implementa Comparable
        return title.compareTo(otroUsuario.getTitle());
    }
}
