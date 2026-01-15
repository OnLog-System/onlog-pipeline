package onlog.consumer.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import onlog.common.model.CanonicalEvent;
import onlog.consumer.ingest.kpi.KpiWriter;
import onlog.consumer.ingest.raw.EnvWriter;
import onlog.consumer.ingest.raw.MachineWriter;
import onlog.consumer.ingest.raw.ScaleWriter;
import onlog.streams.kpi.KpiEvent;

public class Dispatcher {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void dispatch(String topic, String value) throws Exception {

        switch (topic) {

            case "sensor.parsed": {
                CanonicalEvent e =
                        MAPPER.readValue(value, CanonicalEvent.class);

                if ("ENV".equals(e.process)) {
                    EnvWriter.write(e);
                } else if ("QC".equals(e.process)) {
                    ScaleWriter.write(e);
                } else {
                    MachineWriter.write(e);
                }
                break;
            }

            case "kpi.event": {
                KpiEvent k = MAPPER.readValue(value, KpiEvent.class);
                KpiWriter.write(k);
                break;
            }

            default:
                // ignore
        }
    }
}
