package com.linkedin.metadata.config.cache;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class SearchCacheConfiguration {
  SearchLineageCacheConfiguration lineage;
}
