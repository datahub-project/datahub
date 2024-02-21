package io.datahubproject.openapi.v3.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController("EntityControllerV3")
@RequiredArgsConstructor
@RequestMapping("/v3/entity")
@Slf4j
public class EntityController extends io.datahubproject.openapi.v2.controller.EntityController {}
