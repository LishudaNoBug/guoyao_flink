package com.shgl;

public class ShipEntity {

    public String mmsi;
    public String updatetime;
    public String lon;
    public String lat;
    public String course;
    public String speed;
    public String heading;
    public String rot;
    public String status;
    public String static_info_updatetime;
    public String eta;
    public String dest;
    public String destination_tidied;
    public String draught;

    //空参构造函数
    public ShipEntity() {
    }

    //有参构造函数
    public ShipEntity(String mmsi, String updatetime, String lon, String lat, String course, String speed, String heading, String rot, String status, String static_info_updatetime, String eta, String dest, String destination_tidied, String draught) {
        this.mmsi = mmsi;
        this.updatetime = updatetime;
        this.lon = lon;
        this.lat = lat;
        this.course = course;
        this.speed = speed;
        this.heading = heading;
        this.rot = rot;
        this.status = status;
        this.static_info_updatetime = static_info_updatetime;
        this.eta = eta;
        this.dest = dest;
        this.destination_tidied = destination_tidied;
        this.draught = draught;
    }


    @Override
    public String toString() {
        return  mmsi+","+updatetime+","+lon+","+lat+","+course+","+speed+","+heading+","+rot+","
                +status+","+static_info_updatetime+","+eta+","+dest+","+destination_tidied+","+draught+"\n";
    }
}
