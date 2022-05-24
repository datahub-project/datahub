package com.linkedin.metadata.utils;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import lombok.extern.slf4j.Slf4j;

import java.time.Clock;

import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;

@Slf4j
public class AuditStampUtils {
    private AuditStampUtils() {
    }

    public static AuditStamp createDefaultAuditStamp() {
        return new AuditStamp()
                .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))
                .setTime(Clock.systemUTC().millis());
    }
}
