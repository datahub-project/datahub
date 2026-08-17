package io.datahubproject.openapi.openlineage.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.datahubproject.openlineage.model.OpenLineageErrorResponse;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpServletResponseWrapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.springframework.http.MediaType;
import org.springframework.web.filter.OncePerRequestFilter;

public final class OpenLineageAuthenticationErrorFilter extends OncePerRequestFilter {
  private final ObjectMapper objectMapper;

  public OpenLineageAuthenticationErrorFilter() {
    this.objectMapper = new ObjectMapper();
  }

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
      throws ServletException, IOException {
    filterChain.doFilter(
        request,
        new HttpServletResponseWrapper(response) {
          @Override
          public void sendError(int statusCode) throws IOException {
            sendError(statusCode, null);
          }

          @Override
          public void sendError(int statusCode, String message) throws IOException {
            if (statusCode != HttpServletResponse.SC_UNAUTHORIZED) {
              super.sendError(statusCode, message);
              return;
            }

            OpenLineageErrorResponse error = new OpenLineageErrorResponse();
            error.setCode("AUTHENTICATION_REQUIRED");
            error.setMessage("Authentication required");
            error.setDetails(Map.of());

            response.resetBuffer();
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
            response.setCharacterEncoding(StandardCharsets.UTF_8.name());
            objectMapper.writeValue(response.getWriter(), error);
          }
        });
  }
}
