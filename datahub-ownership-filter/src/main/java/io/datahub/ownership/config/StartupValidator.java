package io.datahub.ownership.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

public class StartupValidator implements ApplicationListener<ContextRefreshedEvent> {

    private static final Logger log = LoggerFactory.getLogger(StartupValidator.class);

    private final OwnershipFilterConfiguration.WrapStatus wrapStatus;

    public StartupValidator(OwnershipFilterConfiguration.WrapStatus wrapStatus) {
        this.wrapStatus = wrapStatus;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        if (!wrapStatus.isSuccess()) {
            String msg = "OwnershipInstrumentation was NEVER installed on graphQLEngine. "
                       + "GraphQL search results are UNFILTERED. Refusing to serve traffic.";
            log.error(msg);
            throw new IllegalStateException(msg);
        }
        log.info("OwnershipInstrumentation install verified on context refresh");
    }
}
