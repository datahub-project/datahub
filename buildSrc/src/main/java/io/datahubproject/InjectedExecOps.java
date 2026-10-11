package io.datahubproject;

import javax.inject.Inject;
import org.gradle.process.ExecOperations;

/**
 * Gradle 9 removed {@code Project.exec(...)} and {@code Project.javaexec(...)}. Build scripts must
 * instead go through the injected {@link ExecOperations} service. This interface is instantiated via
 * {@code objects.newInstance(InjectedExecOps.class)} so scripts can call
 * {@code execOps.exec { ... }} / {@code execOps.javaexec { ... }} (see the {@code execOps} extra
 * property wired in the root build's {@code allprojects} block).
 */
public interface InjectedExecOps {
  @Inject
  ExecOperations getExecOperations();
}
