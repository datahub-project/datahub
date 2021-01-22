package com.linkedin.datahub.graphql.resolvers.corpuser;

import com.linkedin.common.url.Url;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.loaders.CorpUserLoader;
import com.linkedin.identity.CorpUser;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.identity.CorpUserInfo;
import graphql.schema.DataFetchingEnvironment;
import org.dataloader.DataLoader;
import org.testng.annotations.Test;

import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class TestManagerResolver {
    private static final ManagerResolver RESOLVER = new ManagerResolver();

    @Test
    public void testResolverUrnNotFound() throws Exception {
        DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);

        QueryContext context = mock(QueryContext.class);
        when(context.isAuthenticated()).thenReturn(true);

        DataLoader mockLoader = mock(DataLoader.class);
        when(mockLoader.load("urn:li:corpuser:missingUser")).thenReturn(
                CompletableFuture.completedFuture(null));

        com.linkedin.datahub.graphql.generated.CorpUserInfo parentInfo = mock(com.linkedin.datahub.graphql.generated.CorpUserInfo.class);
        com.linkedin.datahub.graphql.generated.CorpUser parentUser = mock(com.linkedin.datahub.graphql.generated.CorpUser.class);
        when(parentUser.getUrn()).thenReturn("urn:li:corpuser:missingUser");
        when(parentInfo.getManager()).thenReturn(parentUser);

        when(env.getContext()).thenReturn(context);
        when(env.getSource()).thenReturn(parentInfo);
        when(env.getDataLoader(CorpUserLoader.NAME)).thenReturn(mockLoader);

        assertEquals(RESOLVER.get(env).get(), null);
    }

    @Test
    public void testResolverSuccess() throws Exception {
        DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);

        QueryContext context = mock(QueryContext.class);
        when(context.isAuthenticated()).thenReturn(true);

        DataLoader mockLoader = mock(DataLoader.class);
        when(mockLoader.load("urn:li:corpuser:testUser")).thenReturn(
                CompletableFuture.completedFuture(testUser()));

        com.linkedin.datahub.graphql.generated.CorpUserInfo parentInfo = mock(com.linkedin.datahub.graphql.generated.CorpUserInfo.class);
        com.linkedin.datahub.graphql.generated.CorpUser parentUser = mock(com.linkedin.datahub.graphql.generated.CorpUser.class);
        when(parentUser.getUrn()).thenReturn("urn:li:corpuser:testUser");
        when(parentInfo.getManager()).thenReturn(parentUser);

        when(env.getContext()).thenReturn(context);
        when(env.getSource()).thenReturn(parentInfo);
        when(env.getDataLoader(CorpUserLoader.NAME)).thenReturn(mockLoader);

        CorpUser expectedUser = testUser();
        com.linkedin.datahub.graphql.generated.CorpUser actualUser = RESOLVER.get(env).get();

        assertEquals(actualUser.getUrn(), new CorpuserUrn(expectedUser.getUsername()).toString());
        assertEquals(actualUser.getUsername(), expectedUser.getUsername());
        assertEquals(actualUser.getEditableInfo().getAboutMe(), expectedUser.getEditableInfo().getAboutMe());
        assertEquals(actualUser.getEditableInfo().getPictureLink(), expectedUser.getEditableInfo().getPictureLink().toString());
        assertEquals(actualUser.getEditableInfo().getSkills(), expectedUser.getEditableInfo().getSkills());
        assertEquals(actualUser.getEditableInfo().getTeams(), expectedUser.getEditableInfo().getTeams());
        assertEquals(actualUser.getInfo().getActive(), expectedUser.getInfo().isActive().booleanValue());
        assertEquals(actualUser.getInfo().getManager().getUrn(), expectedUser.getInfo().getManagerUrn().toString());
        assertEquals(actualUser.getInfo().getCountryCode(), expectedUser.getInfo().getCountryCode());
        assertEquals(actualUser.getInfo().getDepartmentId(), expectedUser.getInfo().getDepartmentId());
        assertEquals(actualUser.getInfo().getEmail(), expectedUser.getInfo().getEmail());
        assertEquals(actualUser.getInfo().getDisplayName(), expectedUser.getInfo().getDisplayName());
        assertEquals(actualUser.getInfo().getFirstName(), expectedUser.getInfo().getFirstName());
        assertEquals(actualUser.getInfo().getLastName(), expectedUser.getInfo().getLastName());
        assertEquals(actualUser.getInfo().getFullName(), expectedUser.getInfo().getFullName());
        assertEquals(actualUser.getInfo().getTitle(), expectedUser.getInfo().getTitle());
    }

    private CorpUser testUser() throws URISyntaxException {
        CorpUser validCorpUser = new CorpUser();
        validCorpUser.setUsername("testUser");
        validCorpUser.setEditableInfo(new CorpUserEditableInfo()
                .setAboutMe("Test About Me")
                .setSkills(new StringArray("skill1", "skill2"))
                .setTeams(new StringArray("engineering"))
                .setPictureLink(new Url("www.test.com"))
        );
        validCorpUser.setInfo(new CorpUserInfo()
                .setActive(true)
                .setDisplayName("Test User")
                .setEmail("test@datahub.com")
                .setCountryCode("us")
                .setDepartmentId(1L)
                .setDepartmentName("Engineering")
                .setFirstName("Test")
                .setLastName("User")
                .setManagerUrn(CorpuserUrn.createFromString("urn:li:corpuser:michaelmanager"))
                .setTitle("Engineer")
        );
        return validCorpUser;
    }
}
