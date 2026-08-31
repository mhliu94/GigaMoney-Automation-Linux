package gigamoney;

import com.android.uiautomator.core.Configurator;
import com.android.uiautomator.testrunner.UiAutomatorTestCase;

/** Dumps the active UI hierarchy without waiting for a changing screen to become idle. */
public final class ZeroIdleDump extends UiAutomatorTestCase {
    public void testDump() {
        String output = getParams().getString("output");
        if (output == null || output.length() == 0) {
            throw new IllegalArgumentException("The output parameter is required.");
        }

        Configurator.getInstance().setWaitForIdleTimeout(0L);
        getUiDevice().setCompressedLayoutHeirarchy(true);
        getUiDevice().dumpWindowHierarchy(output);
    }
}
