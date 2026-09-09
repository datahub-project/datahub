# rest.li / pegasus Gradle-9 fork jars

Gradle-9 / Java-25-compatible forks of two **build-only** Gradle plugins, committed here so the
build needs **no Maven publishing** (same in-repo vendor pattern as `vendor/avroutil1-helper-all-fork`,
PR datahub-project/datahub#17189).

| Jar                                                  | Replaces                                                         | Plugin id                        |
| ---------------------------------------------------- | ---------------------------------------------------------------- | -------------------------------- |
| `gradle-plugins-29.74.2-gradle9.jar`                 | `com.linkedin.pegasus:gradle-plugins:29.74.2` (archived)         | `pegasus`                        |
| `gradle-swagger-generator-plugin-2.19.2-gradle9.jar` | `org.hidetake:gradle-swagger-generator-plugin:2.19.2` (archived) | `org.hidetake.swagger.generator` |

Both upstreams are archived and call APIs removed in Gradle 9 (`getConvention`, `getBuildDir`,
`Project.exec`/`javaexec`). The forks migrate those to the supported replacements; behavior is
otherwise unchanged. The pegasus fork still injects its **runtime** codegen tools
(`com.linkedin.pegasus:generator/data/...:29.74.2`) from the normal repos — only the build plugin is forked.

## Wiring

Root `build.gradle` `buildscript` classpath points straight at these jars:

```groovy
classpath files("vendor/rest-li-fork/gradle-plugins-${pegasusVersion}-gradle9.jar")
classpath files("vendor/rest-li-fork/gradle-swagger-generator-plugin-2.19.2-gradle9.jar")
```

No repository, no coordinate, no credentials.

## Integrity (SHA-256)

These jars are Gradle **plugins** — they execute at configuration time with full project access, so
they are a supply-chain surface. Root `build.gradle` verifies these hashes in its `buildscript` block
**before** loading them, and `./gradlew verifyVendorJars` checks them on demand (CI). When you
intentionally rebuild a fork jar, update the hash here **and** in `build.gradle`
(`vendorJarSha256` / the `verifyVendorJars` task).

| Jar                                                  | SHA-256                                                            |
| ---------------------------------------------------- | ------------------------------------------------------------------ |
| `gradle-plugins-29.74.2-gradle9.jar`                 | `a6f48cbb9f4889d274f725ea85db4e818b975d720f03daa80737f7531a5b15f8` |
| `gradle-swagger-generator-plugin-2.19.2-gradle9.jar` | `aa732dc7363f69bcd15c026cee697e5527ac8e1db80e7181e734d859b891ff0a` |

Recompute with: `shasum -a 256 vendor/rest-li-fork/*.jar`

## Sources / rebuild (only if the patch changes — upstreams are frozen)

- pegasus: `github.com/acryldata/rest-li-fork` (branch `gradle-9-compat`) — `gradle-plugins` module.
  Build with **JDK 11** (its Gradle 6.9.4): `./gradlew :gradle-plugins:jar` → copy `build/libs/*.jar` here.
- swagger: the patched `gradle-swagger-generator-plugin`. Build with **JDK 17** (Gradle 7.6.4):
  `./gradlew jar` → copy `build/libs/*.jar` here.
  Bump the `-gradle9` suffix in the filename + `build.gradle` if you change the patch.
