package io.datahubproject.openapi.health;

import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/up")
@RequiredArgsConstructor
@Tag(name = "Health", description = "An API for checking whether openAPI servlet is up.")
public class HealthController {

  @GetMapping(value = "/", produces = MediaType.APPLICATION_JSON_VALUE)
  public ResponseEntity<Boolean> isUp() {
    return ResponseEntity.ok(Boolean.TRUE);
  }
}
