/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import io.openlineage.spark.agent.lifecycle.VisitorFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface Vendors {

  @SuppressWarnings("PMD.AvoidFieldNameMatchingTypeName")
  List<String> VENDORS =
      Arrays.asList(
          // Add vendor classes here
          "io.openlineage.spark.agent.vendor.snowflake.SnowflakeVendor",
          // This is the only chance we have to add the RedshiftVendor to the list of vendors
          "io.openlineage.spark.agent.vendor.redshift.RedshiftVendor");

  static Vendors getVendors() {
    return getVendors(Collections.emptyList());
  }

  static Vendors getVendors(List<String> additionalVendors) {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();

    List<Vendor> vendors =
        Stream.concat(VENDORS.stream(), additionalVendors.stream())
            .map(
                vendorClassName -> {
                  try {
                    Class<?> vendor = cl.loadClass(vendorClassName);
                    return (Vendor) vendor.newInstance();
                  } catch (ClassNotFoundException
                      | InstantiationException
                      | IllegalAccessException e) {
                    return null;
                  }
                })
            .filter(Objects::nonNull)
            .filter(Vendor::isVendorAvailable)
            .collect(Collectors.toList());
    // The main reason to avoid using the service loader and use static loading with the class name
    // is to prevent potential missing loading caused by missing META-INF/services files.
    // This can happen if the user packages the OpenLineage dependency in an Uber-jar without proper
    // services file configuration
    // The implementation with the ClassLoader and the list of vendor class names increase the
    // coupling between the vendor
    // and the app
    // https://github.com/OpenLineage/OpenLineage/issues/1860
    // ServiceLoader<Vendor> serviceLoader = ServiceLoader.load(Vendor.class);
    return new VendorsImpl(vendors);
  }

  static Vendors empty() {
    return new Vendors() {

      @Override
      public Collection<VisitorFactory> getVisitorFactories() {
        return Collections.emptyList();
      }

      @Override
      public Collection<OpenLineageEventHandlerFactory> getEventHandlerFactories() {
        return Collections.emptyList();
      }
    };
  }

  Collection<VisitorFactory> getVisitorFactories();

  Collection<OpenLineageEventHandlerFactory> getEventHandlerFactories();
}
