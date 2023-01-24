package com.linkedin.metadata.boot;

import com.linkedin.metadata.version.GitVersion;
import lombok.Data;

@Data
public class BootstrapManagerArgs {
    GitVersion gitVersion;
    boolean sentryEnabled;
    String sentryDsn;
    String sentryEnv;
}