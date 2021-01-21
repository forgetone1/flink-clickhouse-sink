package ru.ivi.opensource.flinkclickhousesink.model;

public enum ClientProtocol {
    TCP("tcp", 1), HTTP("http", 2);

    private String name;
    private int index;


    private ClientProtocol(String name, int index) {
        this.name = name;
        this.index = index;
    }


    public static String getName(int index) {
        for (ClientProtocol c : ClientProtocol.values()) {
            if (c.getIndex() == index) {
                return c.name;
            }
        }
        return null;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }
}
