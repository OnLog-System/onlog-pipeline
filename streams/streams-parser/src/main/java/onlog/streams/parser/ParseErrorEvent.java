package onlog.streams.parser;

import java.time.Instant;

public class ParseErrorEvent {
    public String raw;
    public String reason;
    public Instant occurredAt;
}
