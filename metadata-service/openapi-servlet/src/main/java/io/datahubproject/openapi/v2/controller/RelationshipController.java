/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.openapi.v2.controller;

import io.datahubproject.openapi.controller.GenericRelationshipController;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/openapi/v2/relationship")
@Slf4j
@Tag(
    name = "Generic Relationships",
    description = "APIs for ingesting and accessing entity relationships.")
public class RelationshipController extends GenericRelationshipController {
  // Supports same methods as GenericRelationshipController.
}
