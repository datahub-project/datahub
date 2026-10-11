package health;

import controllers.ProxyAdmission;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/**
 * Readiness inputs for the management-port probes. The listener that serves them does not share
 * Play's connection table, so it can still answer when the Play server is wedged.
 *
 * <p>Unbound until {@link controllers.ProxyAdmission} is constructed at startup. Until then
 * readiness is {@link Readiness#STARTING} and liveness is still successful.
 */
public final class FrontendProbeState {

  public enum Readiness {
    READY,
    STARTING,
    SHUTTING_DOWN,
    SATURATED
  }

  private static final AtomicReference<ProxyAdmission> ADMISSION = new AtomicReference<>();
  private static final AtomicReference<BooleanSupplier> SHUTTING_DOWN =
      new AtomicReference<>(() -> false);
  private static final AtomicBoolean BOUND = new AtomicBoolean(false);

  private FrontendProbeState() {}

  public static void bind(ProxyAdmission admission, BooleanSupplier shuttingDown) {
    ADMISSION.set(admission);
    SHUTTING_DOWN.set(shuttingDown);
    BOUND.set(true);
  }

  /** Clears startup registration. For unit tests only. */
  public static void resetForTests() {
    ADMISSION.set(null);
    SHUTTING_DOWN.set(() -> false);
    BOUND.set(false);
  }

  public static Readiness readiness() {
    if (!BOUND.get() || ADMISSION.get() == null) {
      return Readiness.STARTING;
    }
    if (SHUTTING_DOWN.get().getAsBoolean()) {
      return Readiness.SHUTTING_DOWN;
    }
    if (!ADMISSION.get().isAcceptingTraffic()) {
      return Readiness.SATURATED;
    }
    return Readiness.READY;
  }
}
