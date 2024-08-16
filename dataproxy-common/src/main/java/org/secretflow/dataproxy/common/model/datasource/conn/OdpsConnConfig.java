package org.secretflow.dataproxy.common.model.datasource.conn;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.validation.constraints.NotBlank;
import lombok.*;
import org.secretflow.dataproxy.common.serializer.SensitiveDataSerializer;

/**
 * connection configuration of odps
 *
 * @author yuexie
 * @date 2024-05-30 10:30:20
 */
@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class OdpsConnConfig implements ConnConfig {

    /**
     * access key id
     */
    @NotBlank
    @JsonSerialize(using = SensitiveDataSerializer.class)
    private String accessKeyId;

    /**
     * access key secret
     */
    @NotBlank
    @JsonSerialize(using = SensitiveDataSerializer.class)
    private String accessKeySecret;

    /**
     * endpoint
     */
    @NotBlank
    private String endpoint;

    /**
     * project name
     */
    private String projectName;

}
