package org.springframework.core.type.classreading;

import org.springframework.core.io.ResourceLoader;

/**
 * Forces ASM-based {@link SimpleMetadataReaderFactory} on JDK 24+ instead of Spring 7's default
 * {@code ClassFileMetadataReaderFactory} (JDK Class-File API).
 *
 * <p>On JDK 24+, Spring 7's multi-release jar selects a Class-File-API metadata reader whose
 * {@code ClassFileAnnotationDelegate} retains the raw {@code java.lang.classfile.Annotation}
 * (holding the class-file {@code byte[]}, parsed constant pool, and class model). During
 * classpath scanning that balloons heap ({@code jdk.internal.classfile.CodeImpl}/{@code Utf8Entry}
 * floods) and OOMs on JDK 25 — see spring-projects/spring-framework#37111. Spring's
 * {@code CachingMetadataReaderFactory} and friends call {@code MetadataReaderFactoryDelegate.create(...)}
 * statically, so this same-FQN class placed earlier on the classpath wins and ASM is selected.
 *
 * <p>Wired into every Spring runtime app module's {@code src/main} (so it lands in
 * {@code BOOT-INF/classes} / {@code WEB-INF/classes}, ahead of {@code spring-core.jar}) plus the
 * Spring-Boot-test modules, via {@code sourceSets…srcDir("$rootDir/gradle/spring-jdk25-shim")}.
 *
 * <p><b>TEMPORARY.</b> Fix merged upstream in spring-projects/spring-framework PR #37112
 * (closes #37111) for Spring Framework 7.0.9. DELETE this shim and all its {@code srcDir}
 * wirings once this project is on spring-framework &gt;= 7.0.9.
 */
abstract class MetadataReaderFactoryDelegate {

    static MetadataReaderFactory create(ResourceLoader resourceLoader) {
        return new SimpleMetadataReaderFactory(resourceLoader);
    }

    static MetadataReaderFactory create(ClassLoader classLoader) {
        return new SimpleMetadataReaderFactory(classLoader);
    }
}
