package io.datahubproject.openapi.openlineage.config;

import static com.linkedin.metadata.Constants.ANONYMOUS_ACTOR_ID;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.datahubproject.openlineage.model.OpenLineageErrorResponse;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.springframework.http.MediaType;
import org.springframework.web.filter.OncePerRequestFilter;

public final class OpenLineageAuthenticationFilter extends OncePerRequestFilter {
  private final ObjectMapper objectMapper;

  public OpenLineageAuthenticationFilter() {
    this.objectMapper = new ObjectMapper();
  }

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
      throws ServletException, IOException {
    Authentication authentication = AuthenticationContext.getAuthentication();
    if (authentication == null
        || authentication.getActor() == null
        || ANONYMOUS_ACTOR_ID.equals(authentication.getActor().getId())) {
      OpenLineageErrorResponse error = new OpenLineageErrorResponse();
      error.setCode("AUTHENTICATION_REQUIRED");
      error.setMessage("Authentication required");
      error.setDetails(Map.of());
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      response.setContentType(MediaType.APPLICATION_JSON_VALUE);
      response.setCharacterEncoding(StandardCharsets.UTF_8.name());
      objectMapper.writeValue(response.getWriter(), error);
      return;
    }
    filterChain.doFilter(request, response);
  }
}
