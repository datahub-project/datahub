package com.linkedin.datahub.graphql;

import com.datahub.authentication.AuthenticationConfiguration;
import com.datahub.authentication.group.GroupService;
import com.datahub.authentication.invite.InviteTokenService;
import com.datahub.authentication.post.PostService;
import com.datahub.authentication.token.StatefulTokenService;
import com.datahub.authentication.user.NativeUserService;
import com.datahub.authorization.AuthorizationConfiguration;
import com.datahub.authorization.role.RoleService;
import com.linkedin.datahub.graphql.analytics.service.AnalyticsService;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.config.DataHubConfiguration;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.metadata.config.TestsConfiguration;
import com.linkedin.metadata.config.ViewsConfiguration;
import com.linkedin.metadata.config.VisualConfiguration;
import com.linkedin.metadata.config.telemetry.TelemetryConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.graph.SiblingGraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.recommendation.RecommendationsService;
import com.linkedin.metadata.secret.SecretService;
import com.linkedin.metadata.service.DataProductService;
import com.linkedin.metadata.service.LineageService;
import com.linkedin.metadata.service.OwnershipTypeService;
import com.linkedin.metadata.service.QueryService;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.metadata.timeline.TimelineService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.usage.UsageClient;
import lombok.Data;

@Data
public class GmsGraphQLEngineArgs {
  EntityClient entityClient;
  SystemEntityClient systemEntityClient;
  GraphClient graphClient;
  UsageClient usageClient;
  AnalyticsService analyticsService;
  EntityService entityService;
  RecommendationsService recommendationsService;
  StatefulTokenService statefulTokenService;
  TimeseriesAspectService timeseriesAspectService;
  EntityRegistry entityRegistry;
  SecretService secretService;
  NativeUserService nativeUserService;
  IngestionConfiguration ingestionConfiguration;
  AuthenticationConfiguration authenticationConfiguration;
  AuthorizationConfiguration authorizationConfiguration;
  GitVersion gitVersion;
  TimelineService timelineService;
  boolean supportsImpactAnalysis;
  VisualConfiguration visualConfiguration;
  TelemetryConfiguration telemetryConfiguration;
  TestsConfiguration testsConfiguration;
  DataHubConfiguration datahubConfiguration;
  ViewsConfiguration viewsConfiguration;
  SiblingGraphService siblingGraphService;
  GroupService groupService;
  RoleService roleService;
  InviteTokenService inviteTokenService;
  PostService postService;
  ViewService viewService;
  OwnershipTypeService ownershipTypeService;
  SettingsService settingsService;
  LineageService lineageService;
  QueryService queryService;
  FeatureFlags featureFlags;
  DataProductService dataProductService;

  // any fork specific args should go below this line
}
