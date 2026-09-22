package filters;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class StaticAssetPathsTest {

  @Test
  void hashedAssetsAndMonacoPathsArePrecompressed() {
    assertTrue(StaticAssetPaths.isPrecompressedStaticPath("/assets/source-By3zWfCx.js"));
    assertTrue(StaticAssetPaths.isPrecompressedStaticPath("/datahub/assets/index-abc.js"));
    assertTrue(
        StaticAssetPaths.isPrecompressedStaticPath(
            "/node_modules/monaco-editor/min/vs/editor/editor.main.js"));
  }

  @Test
  void htmlAndApiPathsStayEligibleForGzipFilter() {
    assertFalse(StaticAssetPaths.isPrecompressedStaticPath("/"));
    assertFalse(StaticAssetPaths.isPrecompressedStaticPath("/search"));
    assertFalse(StaticAssetPaths.isPrecompressedStaticPath("/api/v2/graphql"));
    assertFalse(StaticAssetPaths.isPrecompressedStaticPath("/manifest.json"));
    assertFalse(StaticAssetPaths.isPrecompressedStaticPath(null));
  }
}
