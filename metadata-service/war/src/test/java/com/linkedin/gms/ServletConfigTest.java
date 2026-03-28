package com.linkedin.gms;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.auth.authentication.filter.AuthenticationEnforcementFilter;
import com.datahub.auth.authentication.filter.AuthenticationExtractionFilter;
import com.datahub.gms.servlet.Config;
import com.datahub.gms.servlet.ConfigSearchExport;
import com.datahub.gms.servlet.ReadinessCheck;
import com.linkedin.metadata.config.GMSConfiguration;
import com.linkedin.r2.transport.http.server.RAPJakartaServlet;
import io.datahubproject.openapi.config.TracingInterceptor;
import io.ebean.Database;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.kafka.clients.admin.AdminClient;
import org.neo4j.driver.Driver;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.format.FormatterRegistry;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.InterceptorRegistration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ServletConfigTest {

    private ServletConfig servletConfig;

    @BeforeMethod
    public void setUp() {
        servletConfig = new ServletConfig();

        ReflectionTestUtils.setField(
                servletConfig, "tracingInterceptor", mock(TracingInterceptor.class));
        ReflectionTestUtils.setField(servletConfig, "asyncTimeoutMilliseconds", 5000L);

        GMSConfiguration gmsConfiguration = mock(GMSConfiguration.class);
        when(gmsConfiguration.getBasePathEnabled()).thenReturn(false);
        when(gmsConfiguration.getBasePath()).thenReturn("/gms");
        ReflectionTestUtils.setField(servletConfig, "gmsConfiguration", gmsConfiguration);
    }

    @Test
    public void testAuthExtractionFilterRegistration() {
        AuthenticationExtractionFilter filter = mock(AuthenticationExtractionFilter.class);

        FilterRegistrationBean<AuthenticationExtractionFilter> registration =
                servletConfig.authExtractionFilter(filter);

        assertNotNull(registration);
        assertEquals(registration.getFilter(), filter);
        assertEquals(registration.getOrder(), Integer.valueOf(Integer.MIN_VALUE));
        assertTrue(registration.getUrlPatterns().contains("/*"));

        Object asyncSupported = ReflectionTestUtils.getField(registration, "asyncSupported");
        assertEquals(asyncSupported, true);
    }

    @Test
    public void testAuthFilterRegistration() {
        AuthenticationEnforcementFilter filter = mock(AuthenticationEnforcementFilter.class);

        FilterRegistrationBean<AuthenticationEnforcementFilter> registration =
                servletConfig.authFilter(filter);

        assertNotNull(registration);
        assertEquals(registration.getFilter(), filter);
        assertEquals(registration.getOrder(), Integer.valueOf(Integer.MIN_VALUE + 1));
        assertTrue(registration.getUrlPatterns().contains("/*"));

        Object asyncSupported = ReflectionTestUtils.getField(registration, "asyncSupported");
        assertEquals(asyncSupported, true);
    }

    @Test
    public void testConfigServletRegistration() {
        ServletRegistrationBean<Config> registration = servletConfig.configServlet();

        assertNotNull(registration);
        assertTrue(registration.getUrlMappings().contains("/config"));

        Object loadOnStartup = ReflectionTestUtils.getField(registration, "loadOnStartup");
        assertEquals(loadOnStartup, 15);

        Object asyncSupported = ReflectionTestUtils.getField(registration, "asyncSupported");
        assertEquals(asyncSupported, true);

        Object name = ReflectionTestUtils.getField(registration, "name");
        assertEquals(name, "config");
    }

    @Test
    public void testConfigSearchExportServletRegistration() {
        ServletRegistrationBean<ConfigSearchExport> registration =
                servletConfig.configSearchExportServlet();

        assertNotNull(registration);
        assertTrue(registration.getUrlMappings().contains("/config/search/export"));

        Object loadOnStartup = ReflectionTestUtils.getField(registration, "loadOnStartup");
        assertEquals(loadOnStartup, 15);

        Object asyncSupported = ReflectionTestUtils.getField(registration, "asyncSupported");
        assertEquals(asyncSupported, true);

        Object name = ReflectionTestUtils.getField(registration, "name");
        assertEquals(name, "config-search-export");
    }

    @Test
    public void testReadinessCheckServletRegistration() {
        AdminClient kafkaAdmin = mock(AdminClient.class);
        RestHighLevelClient elasticClient = mock(RestHighLevelClient.class);
        Database database = mock(Database.class);
        Driver neo4jDriver = mock(Driver.class);

        ServletRegistrationBean<ReadinessCheck> registration =
                servletConfig.readinessCheckServlet(kafkaAdmin, elasticClient, database, neo4jDriver);

        assertNotNull(registration);
        assertTrue(registration.getUrlMappings().contains("/health/ready"));

        Object loadOnStartup = ReflectionTestUtils.getField(registration, "loadOnStartup");
        assertEquals(loadOnStartup, 15);

        Object asyncSupported = ReflectionTestUtils.getField(registration, "asyncSupported");
        assertEquals(asyncSupported, true);

        Object name = ReflectionTestUtils.getField(registration, "name");
        assertEquals(name, "readinessCheck");
    }

    @Test
    public void testRestliServletRegistration() {
        RAPJakartaServlet r2Servlet = mock(RAPJakartaServlet.class);

        ServletRegistrationBean<?> registration = servletConfig.restliServletRegistration(r2Servlet);

        assertNotNull(registration);
        assertEquals(registration.getOrder(), Integer.MAX_VALUE);

        Object loadOnStartup = ReflectionTestUtils.getField(registration, "loadOnStartup");
        assertEquals(loadOnStartup, 2);

        Collection<String> mappings = registration.getUrlMappings();
        assertTrue(mappings.contains("/aspects/*"));
        assertTrue(mappings.contains("/entities/*"));
        assertTrue(mappings.contains("/entitiesV2/*"));
        assertTrue(mappings.contains("/entitiesVersionedV2/*"));
        assertTrue(mappings.contains("/usageStats/*"));
        assertTrue(mappings.contains("/platform/*"));
        assertTrue(mappings.contains("/relationships/*"));
        assertTrue(mappings.contains("/analytics/*"));
        assertTrue(mappings.contains("/operations/*"));
        assertTrue(mappings.contains("/runs/*"));
    }

    @Test
    public void testConfigureMessageConverters() {
        List<HttpMessageConverter<?>> converters = new ArrayList<>();

        servletConfig.configureMessageConverters(converters);

        assertFalse(converters.isEmpty());
        assertTrue(
                converters.stream().anyMatch(c -> c instanceof StringHttpMessageConverter),
                "Should register StringHttpMessageConverter");
        assertTrue(
                converters.stream().anyMatch(c -> c instanceof ByteArrayHttpMessageConverter),
                "Should register ByteArrayHttpMessageConverter");
        assertTrue(
                converters.stream().anyMatch(c -> c instanceof FormHttpMessageConverter),
                "Should register FormHttpMessageConverter");
        assertTrue(
                converters.stream().anyMatch(c -> c instanceof MappingJackson2HttpMessageConverter),
                "Should register MappingJackson2HttpMessageConverter");
    }

    @Test
    public void testAddFormatters() {
        FormatterRegistry registry = mock(FormatterRegistry.class);

        servletConfig.addFormatters(registry);

        verify(registry, times(1)).addConverter((Converter<?, ?>) any());
    }

    @Test
    public void testConfigureAsyncSupport() {
        AsyncSupportConfigurer configurer = mock(AsyncSupportConfigurer.class);

        servletConfig.configureAsyncSupport(configurer);

        verify(configurer).setDefaultTimeout(5000L);
    }

    @Test
    public void testAddInterceptors() {
        InterceptorRegistry registry = mock(InterceptorRegistry.class);
        InterceptorRegistration interceptorRegistration = mock(InterceptorRegistration.class);

        when(registry.addInterceptor(any())).thenReturn(interceptorRegistration);
        when(interceptorRegistration.addPathPatterns("/**")).thenReturn(interceptorRegistration);

        servletConfig.addInterceptors(registry);

        verify(registry, times(1)).addInterceptor(any(TracingInterceptor.class));
        verify(interceptorRegistration, times(1)).addPathPatterns("/**");
    }
}
