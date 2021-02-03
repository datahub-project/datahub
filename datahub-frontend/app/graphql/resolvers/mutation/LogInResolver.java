
package graphql.resolvers.mutation;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.datahub.graphql.exception.AuthenticationException;
import com.linkedin.datahub.graphql.exception.ValidationException;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.types.corpuser.CorpUserType;
import graphql.PlayQueryContext;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import org.apache.commons.lang3.StringUtils;
import security.AuthUtil;
import security.AuthenticationManager;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CompletableFuture;

import static security.AuthConstants.*;

/**
 * Resolver responsible for authenticating a user
 */
public class LogInResolver implements DataFetcher<CompletableFuture<CorpUser>> {

    private final CorpUserType _corpUserType;

    public LogInResolver(final CorpUserType corpUserType) {
        _corpUserType = corpUserType;
    }

    @Override
    public CompletableFuture<CorpUser> get(DataFetchingEnvironment environment) throws Exception {
        /*
            Extract arguments
         */
        final String username = environment.getArgument(USER_NAME);
        final String password = environment.getArgument(PASSWORD);

        if (StringUtils.isBlank(username)) {
            throw new ValidationException("username must not be empty");
        }

        PlayQueryContext context = environment.getContext();
        context.getSession().clear();

        // Create a uuid string for this session if one doesn't already exist
        String uuid = context.getSession().get(UUID);
        if (uuid == null) {
            uuid = java.util.UUID.randomUUID().toString();
            context.getSession().put(UUID, uuid);
        }

        try {
            AuthenticationManager.authenticateUser(username, password);
        } catch (javax.naming.AuthenticationException e) {
            throw new AuthenticationException("Failed to authenticate user", e);
        }

        context.getSession().put(USER, username);

        String secretKey = context.getAppConfig().getString(SECRET_KEY_PROPERTY);
        try {
            // store hashed username within PLAY_SESSION cookie
            String hashedUserName = AuthUtil.generateHash(username, secretKey.getBytes());
            context.getSession().put(AUTH_TOKEN, hashedUserName);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException("Failed to hash username", e);
        }

        /*
            Fetch the latest version of the logged in user.
         */
        final String urn = new CorpuserUrn(username).toString();
        return environment.getDataLoaderRegistry().getDataLoader(_corpUserType.name()).load(urn).thenApply(corpUserObj -> (CorpUser) corpUserObj);
    }
}
