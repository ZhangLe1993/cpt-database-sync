package com.aihuishou.bi.sync;

import lombok.Data;

@Data
public class MetaInfo {
    private String mysqlDb;
    private String table;
    private String targetDb;
}
