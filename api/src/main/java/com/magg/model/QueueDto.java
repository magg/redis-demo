package com.magg.model;

import java.io.Serializable;

public class QueueDto implements Serializable
{

    private String value;
    private String name;


    public QueueDto(String value, String name)
    {
        this.value = value;
        this.name = name;
    }


    public String getName()
    {
        return name;
    }


    public void setName(String name)
    {
        this.name = name;
    }

    public String getValue()
    {
        return value;
    }


    public void setValue(String value)
    {
        this.value = value;
    }
}