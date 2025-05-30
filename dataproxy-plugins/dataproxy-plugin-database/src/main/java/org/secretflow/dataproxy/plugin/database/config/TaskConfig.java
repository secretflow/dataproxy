package org.secretflow.dataproxy.plugin.database.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.secretflow.dataproxy.plugin.database.reader.DatabaseDoGetContext;


@Getter
@ToString
public class TaskConfig {

    private final long startIndex;
    private final long count;

    @JsonIgnore
    private final DatabaseDoGetContext context;

    @Setter
    private long currentIndex;

    private final boolean compress;

    @Getter
    @Setter
    private Throwable error;

    public TaskConfig(DatabaseDoGetContext context, long startIndex, long count) {
        this(context, startIndex, count, true);
    }

    public TaskConfig(DatabaseDoGetContext context, long startIndex, long count, boolean compress) {
        this.context = context;
        this.startIndex = startIndex;
        this.count = count;
        this.compress = compress;
        this.currentIndex = startIndex;
    }
}
