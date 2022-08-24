package com.tidb.jdbc.impl;

import java.util.HashMap;
import java.util.Map;

public class Bankend {

    private String[] bankend;

    private Map<String,Weight> weightBankend = new HashMap<>();


    public String[] getBankend() {
        return bankend;
    }

    public void setBankend(String[] bankend) {
        this.bankend = bankend;
    }

    public Map<String, Weight> getWeightBankend() {
        return weightBankend;
    }

    public void setWeightBankend(Map<String, Weight> weightBankend) {
        this.weightBankend = weightBankend;
    }
}
