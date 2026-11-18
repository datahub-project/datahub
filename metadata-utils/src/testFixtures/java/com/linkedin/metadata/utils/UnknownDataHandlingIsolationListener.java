package com.linkedin.metadata.utils;

import org.testng.IClassListener;
import org.testng.ITestClass;

/**
 * Removes the unknown-data handling override {@link UnknownDataHandlingsForTests#use} set, after
 * each test class, so a class that forgets to reset can't leave it for the next one. The installed
 * handling is never touched. {@link UnknownDataHandlingsForTests#use} refuses the override in tests
 * that run classes alongside, so no other class is running when this removes it.
 *
 * <p>Loaded through {@code META-INF/services}, so every TestNG run with these test fixtures on its
 * classpath uses it.
 */
public class UnknownDataHandlingIsolationListener implements IClassListener {

  @Override
  public void onBeforeClass(ITestClass testClass) {}

  @Override
  public void onAfterClass(ITestClass testClass) {
    UnknownDataHandlings.reset();
  }
}
