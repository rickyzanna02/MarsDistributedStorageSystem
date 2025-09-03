package org.example;

public class Config {

    // Numero di repliche per nodo
    public static final int N = 3;

    // Quorum di lettura R
    public static final int R = 2;

    // Quorum di scrittura W
    public static final int W = 2;

    // Timeout massimo in millisecondi per ottenere R o W risposte
    public static final int T = 2000;

    // Ritardo minimo e massimo (in ms) per simulare rete
    public static final int MIN_DELAY_MS = 10;
    public static final int MAX_DELAY_MS = 50;
}

