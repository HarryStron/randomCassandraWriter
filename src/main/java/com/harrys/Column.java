package com.harrys;

import nl.flotsam.xeger.Xeger;

class Column {
    private String name;
    private String regexp;

    Column(String name, String regexp) {
        this.name = name;
        this.regexp = regexp;
    }

    String generateRandomValue() {
        Xeger generator = new Xeger(regexp);
        return generator.generate();
    }

    String getName() {
        return name;
    }
}
