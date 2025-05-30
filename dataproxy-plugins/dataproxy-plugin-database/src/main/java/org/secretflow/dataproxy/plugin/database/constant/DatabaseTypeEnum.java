package org.secretflow.dataproxy.plugin.database.constant;

import lombok.Getter;

@Getter
public enum DatabaseTypeEnum {
    TABLE("table"),

    SQL("sql");

    private final String type;

    DatabaseTypeEnum(String type) {
        this.type = type;
    }
}
