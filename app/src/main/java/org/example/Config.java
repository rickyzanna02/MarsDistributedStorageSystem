// CONFIG.JAVA: contains configuration parameters for the system.

package org.example;

public class Config {
   
    public static final int NODES = 3; // # nodes in the system
    public static final int N = 3; // # replicas per node
    public static final int R = 2; // quorum R
    public static final int W = 2; // quorum W
    public static final int T = 1000; // quorum timeout
    public static final int MIN_DELAY_MS = 50; //min network delay
    public static final int MAX_DELAY_MS = 300; //max network delay
    public static final int ENDTESTPAUSE = 2000; //end test pause
}

