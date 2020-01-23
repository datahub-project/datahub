package com.linkedin.datahub.dao.view;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.util.RestliUtil;
import com.linkedin.identity.CorpUser;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.identity.client.CorpUsers;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class CorpUserViewDaoTest {
    private CorpUsers _corpUsers;
    private CorpUserViewDao _corpUserViewDao;

    @BeforeClass
    public void init() {
        _corpUsers = mock(CorpUsers.class);
        _corpUserViewDao = new CorpUserViewDao(_corpUsers);
    }

    @Test
    public void testGetManagerName() throws Exception {
        String testUser = "testuser";
        String testManager = "testManager";
        CorpuserUrn corpUserUrn = new CorpuserUrn(testUser);
        CorpuserUrn corpUserManagerUrn = new CorpuserUrn(testManager);

        CorpUserEditableInfo corpUserEditableInfo = new CorpUserEditableInfo()
                .setAboutMe("I am a test user")
                .setSkills(new StringArray(Arrays.asList("skill1", "skill2")))
                .setTeams(new StringArray(Arrays.asList("team1", "team2")));
        CorpUser corpManager = new CorpUser().setUsername(testManager)
                .setInfo(new CorpUserInfo().setActive(true).setFullName("Test Manager").setEmail("testManager@linkedin.com"));
        when(_corpUsers.get(corpUserManagerUrn)).thenReturn(corpManager);

        // test 1: corp user has the manager urn
        CorpUserInfo corpUserInfo1 = new CorpUserInfo()
                .setActive(true)
                .setDisplayName("Test User")
                .setEmail("testuser@linkedin.com")
                .setManagerUrn(corpUserManagerUrn);
        CorpUser corpUser1 =  new CorpUser().setUsername(testUser)
                .setEditableInfo(corpUserEditableInfo)
                .setInfo(corpUserInfo1);

        when(_corpUsers.get(corpUserUrn)).thenReturn(corpUser1);

        JsonNode node1 = RestliUtil.toJsonNode(_corpUserViewDao.get(corpUserUrn.toString()));
        assertEquals(node1.get("username").textValue(), testUser);
        assertEquals(node1.get("info").get("managerName").textValue(), "Test Manager");

        // test 2:  corp user does not have manager urn
        CorpUserInfo corpUserInfo2 = new CorpUserInfo()
                .setActive(true)
                .setDisplayName("Test User")
                .setEmail("testuser@linkedin.com");
        CorpUser corpUser2 = new CorpUser().setUsername(testUser)
                .setEditableInfo(corpUserEditableInfo)
                .setInfo(corpUserInfo2);
        when(_corpUsers.get(corpUserUrn)).thenReturn(corpUser2);
        JsonNode node2 = RestliUtil.toJsonNode(_corpUserViewDao.get(corpUserUrn.toString()));
        assertEquals(node2.get("username").textValue(), testUser);
        assertNull(node2.get("info").get("managerName"));

        // test 3: corp user does not have info set
        CorpUser corpUser3 = new CorpUser().setUsername(testUser)
                .setEditableInfo(corpUserEditableInfo);
        when(_corpUsers.get(corpUserUrn)).thenReturn(corpUser3);
        JsonNode node3 = RestliUtil.toJsonNode(_corpUserViewDao.get(corpUserUrn.toString()));
        assertEquals(node3.get("username").textValue(), testUser);
        assertNull(node3.get("info"));
    }
}