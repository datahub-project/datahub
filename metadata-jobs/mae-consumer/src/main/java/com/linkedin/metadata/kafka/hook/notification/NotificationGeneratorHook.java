package com.linkedin.metadata.kafka.hook.notification;

import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.mxe.MetadataChangeLog;
import javax.annotation.Nonnull;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Singleton
public class NotificationGeneratorHook implements MetadataChangeLogHook {

  public NotificationGeneratorHook() {
  }

  @Override
  public void init() {
  }

  @Override
  public void invoke(@Nonnull MetadataChangeLog event) {
    // TODO: Generate notification requests on change stream.
  }
}