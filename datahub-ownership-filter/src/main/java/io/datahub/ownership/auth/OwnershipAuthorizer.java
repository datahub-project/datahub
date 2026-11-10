package io.datahub.ownership.auth;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerContext;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.Owner;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.identity.GroupMembership;
import io.datahub.ownership.admin.AdminBypass;
import io.datahubproject.metadata.context.OperationContext;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.logging.Logger;

public class OwnershipAuthorizer implements Authorizer {

    private static final Logger log = Logger.getLogger(OwnershipAuthorizer.class.getName());

    /** Key used to pass the {@link SystemEntityClient} via {@link AuthorizerContext#data()}. */
    public static final String CTX_ENTITY_CLIENT = "entityClient";

    /** Key used to pass the system {@link OperationContext} via {@link AuthorizerContext#data()}. */
    public static final String CTX_SYSTEM_OP_CONTEXT = "systemOpContext";

    private static final String DISABLED_REASON =
        "Plugin disabled — EntityClient not available";

    private static final Set<String> DEFAULT_GATED_PRIVILEGES = Set.of(
        "VIEW_ENTITY_PAGE", "GET_ENTITY", "VIEW_DATASET_USAGE", "VIEW_DATASET_PROFILE"
    );

    private Function<String, Set<String>> ownershipResolver;
    private Function<Urn, List<Urn>> groupsResolver;
    private AdminBypass adminBypass;
    private Set<String> gatedPrivileges = DEFAULT_GATED_PRIVILEGES;
    /** True when EntityClient/OperationContext were absent at init time; plugin abstains entirely. */
    private boolean disabledMode = false;

    @Override
    public void init(@Nonnull Map<String, Object> authorizerConfig, @Nonnull AuthorizerContext ctx) {
        SystemEntityClient entityClient =
            (SystemEntityClient) ctx.data().get(CTX_ENTITY_CLIENT);
        OperationContext systemOpContext =
            (OperationContext) ctx.data().get(CTX_SYSTEM_OP_CONTEXT);

        if (entityClient == null || systemOpContext == null) {
            this.disabledMode = true;
            log.warning("OwnershipAuthorizer: EntityClient/OperationContext not found in "
                + "AuthorizerContext.data() — direct entity-page ownership checks are DISABLED. "
                + "Search/browse/lineage filtering via OwnershipInstrumentation is unaffected. "
                + "To wire entity-page checks, upstream DataHub must populate `entityClient`/"
                + "`systemOpContext` keys in AuthorizerContext (or this plugin must be re-architected).");
            // Still wire admin bypass and gated privileges — they don't require EntityClient.
        } else {
            this.ownershipResolver = entityUrn -> {
                try {
                    Urn urn = Urn.createFromString(entityUrn);
                    var resp = entityClient.getV2(systemOpContext, urn, Set.of("ownership"));
                    if (resp == null || resp.getAspects() == null) return Set.of();
                    var ea = resp.getAspects().get("ownership");
                    if (ea == null) return Set.of();
                    var ownership = new Ownership(ea.getValue().data());
                    Set<String> result = new HashSet<>();
                    for (Owner owner : ownership.getOwners()) {
                        result.add(owner.getOwner().toString());
                    }
                    return result;
                } catch (Exception e) {
                    throw new RuntimeException("Ownership lookup failed for " + entityUrn, e);
                }
            };

            this.groupsResolver = actor -> {
                try {
                    var resp = entityClient.getV2(systemOpContext, actor, Set.of("groupMembership"));
                    if (resp == null || resp.getAspects() == null) return List.of();
                    var ea = resp.getAspects().get("groupMembership");
                    if (ea == null) return List.of();
                    var gm = new GroupMembership(ea.getValue().data());
                    List<Urn> result = new ArrayList<>();
                    for (Urn g : gm.getGroups()) {
                        result.add(g);
                    }
                    return result;
                } catch (Exception e) {
                    throw new RuntimeException("Group lookup failed for " + actor, e);
                }
            };
        }

        this.adminBypass = new AdminBypass(
            parseUrnCsv((String) authorizerConfig.getOrDefault("adminUserUrns", "urn:li:corpuser:datahub")),
            parseUrnCsv((String) authorizerConfig.getOrDefault("adminGroupUrns", "urn:li:corpGroup:admins")));

        Object gp = authorizerConfig.get("gatedPrivileges");
        if (gp instanceof String s && !s.isBlank()) {
            this.gatedPrivileges = Set.of(s.split(","));
        }
    }

    private static Set<Urn> parseUrnCsv(String csv) {
        if (csv == null || csv.isBlank()) return Set.of();
        Set<Urn> out = new HashSet<>();
        for (String s : csv.split(",")) {
            try {
                out.add(Urn.createFromString(s.trim()));
            } catch (Exception e) {
                log.warning("OwnershipAuthorizer: ignoring malformed URN " + s.trim() + ": " + e.getMessage());
            }
        }
        return out;
    }

    void setOwnershipResolver(Function<String, Set<String>> r) {
        this.ownershipResolver = r;
    }

    void setGroupsResolver(Function<Urn, List<Urn>> r) {
        this.groupsResolver = r;
    }

    void setAdminBypass(AdminBypass b) {
        this.adminBypass = b;
    }

    void setGatedPrivileges(Set<String> p) {
        this.gatedPrivileges = p;
    }

    @Override
    public AuthorizationResult authorize(@Nonnull AuthorizationRequest request) {
        if (disabledMode) {
            return new AuthorizationResult(request, AuthorizationResult.Type.ALLOW, DISABLED_REASON);
        }
        if (!gatedPrivileges.contains(request.getPrivilege())) {
            return new AuthorizationResult(request, AuthorizationResult.Type.ALLOW, "Not ownership-gated");
        }
        if (request.getResourceSpec().isEmpty()) {
            return new AuthorizationResult(request, AuthorizationResult.Type.ALLOW, "No resource");
        }

        Urn actor;
        try {
            actor = Urn.createFromString(request.getActorUrn());
        } catch (Exception e) {
            return new AuthorizationResult(request, AuthorizationResult.Type.DENY, "Bad actor URN");
        }

        List<Urn> groups = groupsResolver.apply(actor);
        if (adminBypass.isAdmin(actor, groups)) {
            return new AuthorizationResult(request, AuthorizationResult.Type.ALLOW, "Admin bypass");
        }

        String entityUrn = request.getResourceSpec().get().getEntity();
        Set<String> owners = ownershipResolver.apply(entityUrn);
        if (owners.contains(actor.toString())) {
            return new AuthorizationResult(request, AuthorizationResult.Type.ALLOW, "Direct owner");
        }
        for (Urn g : groups) {
            if (owners.contains(g.toString())) {
                return new AuthorizationResult(request, AuthorizationResult.Type.ALLOW, "Group owner");
            }
        }
        return new AuthorizationResult(request, AuthorizationResult.Type.DENY, "Not owner");
    }
}
