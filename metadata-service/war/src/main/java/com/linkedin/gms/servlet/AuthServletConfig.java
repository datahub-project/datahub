package com.linkedin.gms.servlet;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@ComponentScan(basePackages = {"com.datahub.auth.authentication", "com.datahub.authorization"})
@Configuration
public class AuthServletConfig {}
