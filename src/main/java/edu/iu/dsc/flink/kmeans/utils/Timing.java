package edu.iu.dsc.flink.kmeans.utils;

import java.io.Serializable;
import java.util.Map;

public class Timing implements Serializable {
    public Map<Integer, Long> current;
    public Map<Integer, Long> previous;
    public int id;
}
