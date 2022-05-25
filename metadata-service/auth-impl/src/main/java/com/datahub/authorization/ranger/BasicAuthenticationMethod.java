package com.datahub.authorization.ranger;

import lombok.Data;

/**
 * Use username and password to authenticate service with Apache Ranger
 */
@Data
public class BasicAuthenticationMethod extends AuthenticationMethod {
    private String rangerUsername;
    private String rangerPassword;

}
