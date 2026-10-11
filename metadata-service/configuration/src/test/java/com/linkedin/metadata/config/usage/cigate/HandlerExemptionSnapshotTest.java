package com.linkedin.metadata.config.usage.cigate;

import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HandlerExemptionSnapshotTest {

  @Test
  public void testReconcileRefreshesLineNumberAndDropsInstrumentedEntries() {
    HandlerInstrumentationSurface scanned =
        new HandlerInstrumentationSurface(
            List.of(
                new HandlerInstrumentationSurface.HandlerEntry("Demo.java", 12, false, null),
                new HandlerInstrumentationSurface.HandlerEntry("Other.java", 20, true, "read")));
    HandlerExemptionSnapshot existing =
        new HandlerExemptionSnapshot(
            List.of(
                new HandlerExemptionSnapshot.Exemption("Demo.java", 10, "still exempt"),
                new HandlerExemptionSnapshot.Exemption("Other.java", 20, "was exempt")));
    HandlerExemptionSnapshot reconciled = HandlerExemptionSnapshot.reconcile(scanned, existing);
    Assert.assertEquals(reconciled.exemptions().size(), 1);
    Assert.assertEquals(reconciled.exemptions().get(0).sourceFile(), "Demo.java");
    Assert.assertEquals(reconciled.exemptions().get(0).lineNumber(), Integer.valueOf(12));
  }
}
