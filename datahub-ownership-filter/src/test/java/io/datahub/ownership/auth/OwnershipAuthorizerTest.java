package io.datahub.ownership.auth;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerContext;
import com.datahub.authorization.EntitySpec;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import io.datahub.ownership.admin.AdminBypass;
import io.datahubproject.metadata.context.OperationContext;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OwnershipAuthorizerTest {

    @Test
    void deniesNonOwnerEntityRead() {
        OwnershipAuthorizer auth = new OwnershipAuthorizer();
        auth.setOwnershipResolver(urn -> Set.of("urn:li:corpuser:owner"));
        auth.setAdminBypass(new AdminBypass(Set.of(), Set.of()));
        auth.setGroupsResolver(actor -> List.of());

        AuthorizationRequest req = new AuthorizationRequest(
            "urn:li:corpuser:alice",
            "VIEW_ENTITY_PAGE",
            Optional.of(new EntitySpec("dataset", "urn:li:dataset:(urn:li:dataPlatform:mysql,a.b,PROD)")),
            Collections.emptyList());

        assertThat(auth.authorize(req).getType()).isEqualTo(AuthorizationResult.Type.DENY);
    }

    @Test
    void allowsOwnerEntityRead() {
        OwnershipAuthorizer auth = new OwnershipAuthorizer();
        auth.setOwnershipResolver(urn -> Set.of("urn:li:corpuser:alice"));
        auth.setAdminBypass(new AdminBypass(Set.of(), Set.of()));
        auth.setGroupsResolver(actor -> List.of());

        AuthorizationRequest req = new AuthorizationRequest(
            "urn:li:corpuser:alice",
            "VIEW_ENTITY_PAGE",
            Optional.of(new EntitySpec("dataset", "urn:li:dataset:(urn:li:dataPlatform:mysql,a.b,PROD)")),
            Collections.emptyList());

        assertThat(auth.authorize(req).getType()).isEqualTo(AuthorizationResult.Type.ALLOW);
    }

    @Test
    void allowsNonOwnershipGatedPrivileges() {
        OwnershipAuthorizer auth = new OwnershipAuthorizer();
        auth.setOwnershipResolver(urn -> Set.of());
        auth.setAdminBypass(new AdminBypass(Set.of(), Set.of()));
        auth.setGroupsResolver(actor -> List.of());

        AuthorizationRequest req = new AuthorizationRequest(
            "urn:li:corpuser:alice",
            "MANAGE_USERS_AND_GROUPS",
            Optional.empty(),
            Collections.emptyList());

        assertThat(auth.authorize(req).getType()).isNotEqualTo(AuthorizationResult.Type.DENY);
    }

    @Test
    void initWithoutEntityClientAbstains() {
        // Simulate the production case: AuthorizerChainFactory provides an empty ctx.data() map.
        var ctx = new AuthorizerContext(Map.of(), null);

        OwnershipAuthorizer auth = new OwnershipAuthorizer();
        auth.init(Map.of(
            "adminUserUrns", "urn:li:corpuser:datahub",
            "adminGroupUrns", "urn:li:corpGroup:admins",
            "gatedPrivileges", "VIEW_ENTITY_PAGE"), ctx);

        // A request that would normally be DENIED (alice is not the owner of anything).
        AuthorizationRequest req = new AuthorizationRequest(
            "urn:li:corpuser:alice",
            "VIEW_ENTITY_PAGE",
            Optional.of(new EntitySpec("dataset", "urn:li:dataset:(urn:li:dataPlatform:mysql,a.b,PROD)")),
            Collections.emptyList());

        AuthorizationResult result = auth.authorize(req);
        assertThat(result.getType()).isEqualTo(AuthorizationResult.Type.ALLOW);
        assertThat(result.getMessage()).contains("Plugin disabled");
    }

    @Test
    void initWiresOwnershipFromEntityClient() throws Exception {
        var ownership = new Ownership().setOwners(
            new OwnerArray(
                new Owner()
                    .setOwner(Urn.createFromString("urn:li:corpuser:alice"))
                    .setType(OwnershipType.DATAOWNER)));

        var ea = new EnvelopedAspect().setValue(new Aspect(ownership.data()));
        var aspectMap = new EnvelopedAspectMap(Map.of("ownership", ea));
        var entityResponse = new EntityResponse().setAspects(aspectMap);

        var entityClient = mock(SystemEntityClient.class);
        when(entityClient.getV2(any(), any(), any())).thenReturn(entityResponse);

        var opContext = mock(OperationContext.class);

        var ctx = new AuthorizerContext(
            Map.of(
                OwnershipAuthorizer.CTX_ENTITY_CLIENT, entityClient,
                OwnershipAuthorizer.CTX_SYSTEM_OP_CONTEXT, opContext),
            null);

        OwnershipAuthorizer auth = new OwnershipAuthorizer();
        auth.init(Map.of(
            "adminUserUrns", "urn:li:corpuser:datahub",
            "adminGroupUrns", "urn:li:corpGroup:admins",
            "gatedPrivileges", "VIEW_ENTITY_PAGE"), ctx);

        AuthorizationRequest req = new AuthorizationRequest(
            "urn:li:corpuser:alice",
            "VIEW_ENTITY_PAGE",
            Optional.of(new EntitySpec("dataset", "urn:li:dataset:(urn:li:dataPlatform:mysql,a.b,PROD)")),
            Collections.emptyList());

        assertThat(auth.authorize(req).getType()).isEqualTo(AuthorizationResult.Type.ALLOW);
    }
}
