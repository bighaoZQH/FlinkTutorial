package org.bighao.apitest.source.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @version 1.0
 * @author: bighao周启豪
 * @date 2021/3/21 0:47
 *
 * 传感器温度读数的数据类型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorReading {

    private String id;
    private Long timestamp;
    // 温度值
    private Double temperature;

}
