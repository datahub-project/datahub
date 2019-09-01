package com.linkedin.metadata.dao.retention;

import lombok.Value;


/**
 * A retention policy that retains every version of metadata aspects.
 */
@Value
public class IndefiniteRetention implements Retention {

}
