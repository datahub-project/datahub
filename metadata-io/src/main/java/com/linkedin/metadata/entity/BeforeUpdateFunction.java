package com.linkedin.metadata.entity;

import com.linkedin.data.template.RecordTemplate;

import java.util.Optional;
import java.util.function.BiFunction;

public interface BeforeUpdateFunction extends BiFunction<Optional<RecordTemplate>, RecordTemplate, RecordTemplate> {
}

