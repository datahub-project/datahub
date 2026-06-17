package com.datahub.authorization.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Configuration for system entity data access control (API gate, DAO filter, write validator). */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SystemDataAccessControlConfiguration {
  private boolean enabled = true;
}
