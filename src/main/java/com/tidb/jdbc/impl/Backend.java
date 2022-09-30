package com.tidb.jdbc.impl;

import java.util.HashMap;
import java.util.Map;

public class Backend {

    private String[] backend;

    private Map<String,Weight> weightBackend = new HashMap<>();


    public String[] getBackend() {
        return backend;
    }

    public void setBackend(String[] backend) {
        this.backend = backend;
    }

    public Map<String, Weight> getWeightBackend() {
        return weightBackend;
    }

    public void setWeightBackend(Map<String, Weight> weightBackend) {
        this.weightBackend = weightBackend;
    }
}
