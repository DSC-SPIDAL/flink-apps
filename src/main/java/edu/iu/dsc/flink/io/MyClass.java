package edu.iu.dsc.flink.io;

import java.io.Serializable;

public class MyClass implements Serializable {
    private int id;
    private String name;

    public MyClass() {
    }

    public MyClass(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }
}
