package com.datahub.authorization.ranger;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class AuthorizerConfig {
  private String username;
  private String password;
}
