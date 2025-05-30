package org.secretflow.dataproxy.plugin.database.config;

public class DatabaseConfigConstant {
    public static class ConfigKey {

        /**
         * If this value is set, the table is created according to this value.<br>
         * if it is not set, it is empty by default, and if ODPS must require a lifecycle and the value is not set, the creation fails. <br>
         *
         * This value is the value of the ODPS table lifecycle, and the unit is days.<br>
         * 1~37231, the default value is 37231.<br>
         * The value of this parameter is used to determine whether the table is expired.<br>
         * If the table is expired, it will be deleted.
         */
        public static final String HIVE_TABLE_LIFECYCLE_VALUE = "HIVE_TABLE_LIFECYCLE_VALUE";
        public static final String ORACLE_TABLE_LIFECYCLE_VALUE = "ORACLE_TABLE_LIFECYCLE_VALUE";
    }
}
