package com.demo.db.bachend.utils;

public class Panic {
    public static void panic(Exception err) {
        err.printStackTrace();
        System.exit(1);
    }
}
