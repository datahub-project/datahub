package com.linkedin.metadata.datahubusage.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@NoArgsConstructor(force = true, access = AccessLevel.PROTECTED)
@JsonIgnoreProperties(ignoreUnknown = true)
@SuperBuilder
public class CreateAccessTokenEvent extends UpdateAspectEvent {}
