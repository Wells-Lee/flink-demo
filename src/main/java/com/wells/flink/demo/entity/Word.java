package com.wells.flink.demo.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Description 
 * Created by wells on 2020-05-26 09:59:47
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Word {
    /**
     * word
     */
    public String word;

    /**
     * 出现的次数
     */
    public Long count;

}
