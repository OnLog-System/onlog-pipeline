// Not Used

package time;

import java.time.Instant;

public class TimeSlot {

    public static Instant currentSlot() {
        long now = Instant.now().getEpochSecond();
        long slot = (now / 10) * 10;
        return Instant.ofEpochSecond(slot);
    }
}
