package com.tidb.jdbc.impl;

import java.util.HashMap;
import java.util.Map;

public class Bankend {

    private String[] bankend;

    private Map<String,Integer> weightBankend = new HashMap<>();


    public String[] getBankend() {
        return bankend;
    }

    public void setBankend(String[] bankend) {
        this.bankend = bankend;
    }

    public Map<String, Integer> getWeightBankend() {
        return weightBankend;
    }

    public void setWeightBankend(Map<String, Integer> weightBankend) {
        this.weightBankend = weightBankend;
    }
}
