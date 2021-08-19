package com.datahub.metadata.graphql;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class GraphQLController {
  @PostMapping("/graphql")
  void postGraphQL() {
    System.out.println("POST am graphql!");
  }

  @GetMapping("/graphql")
  void getGraphQL() {
    System.out.println("GET am graphql!");
  }
}
