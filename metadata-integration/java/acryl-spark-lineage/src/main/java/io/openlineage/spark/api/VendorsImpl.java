/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import io.openlineage.spark.agent.lifecycle.VisitorFactory;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class VendorsImpl implements Vendors {
  private final List<Vendor> vendors;
  private final VendorsContext vendorsContext;

  public VendorsImpl(List<Vendor> vendors, VendorsContext vendorsContext) {
    this.vendors = vendors;
    this.vendorsContext = vendorsContext;
  }

  @Override
  public Collection<VisitorFactory> getVisitorFactories() {
    return vendors.stream()
        .map(Vendor::getVisitorFactory)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
  }

  @Override
  public Collection<OpenLineageEventHandlerFactory> getEventHandlerFactories() {
    return vendors.stream()
        .map(Vendor::getEventHandlerFactory)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
  }

  @Override
  public VendorsContext getVendorsContext() {
    return vendorsContext;
  }
}
