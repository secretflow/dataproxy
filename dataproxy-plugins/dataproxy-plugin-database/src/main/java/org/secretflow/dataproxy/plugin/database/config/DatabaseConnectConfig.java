package org.secretflow.dataproxy.plugin.database.config;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.secretflow.dataproxy.common.serializer.SensitiveDataSerializer;

/**
 *
 * @param username database username
 * @param password database password
 * @param endpoint database endpoint
 * @param database database name
 */
public record DatabaseConnectConfig(@JsonSerialize(using = SensitiveDataSerializer.class) String username,
                                    @JsonSerialize(using = SensitiveDataSerializer.class) String password,
                                    String endpoint, String database) {
}
