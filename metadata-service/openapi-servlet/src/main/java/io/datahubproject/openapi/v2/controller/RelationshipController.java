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
