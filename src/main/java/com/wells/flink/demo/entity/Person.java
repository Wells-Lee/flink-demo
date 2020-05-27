package com.wells.flink.demo.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Description 
 * Created by wells on 2020-05-06 20:03:01
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Person implements Serializable {
    public String name;
    public Integer age;
    public String sex;
    public Long timestamp;
}
