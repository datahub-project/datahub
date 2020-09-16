package com.linkedin.metadata.builders.search;

import com.linkedin.common.CorpGroupUrnArray;
import com.linkedin.common.CorpuserUrnArray;
import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.identity.CorpGroupInfo;
import com.linkedin.metadata.aspect.CorpGroupAspect;
import com.linkedin.metadata.aspect.CorpGroupAspectArray;
import com.linkedin.metadata.search.CorpGroupDocument;
import com.linkedin.metadata.snapshot.CorpGroupSnapshot;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class CorpGroupIndexBuilderTest {

  @Test
  public void testGetDocumentsToUpdateFromCorpGroupSnapshot() {
    CorpGroupUrn corpGroupUrn = new CorpGroupUrn("foo");
    CorpGroupSnapshot corpGroupSnapshot = new CorpGroupSnapshot().setUrn(corpGroupUrn).setAspects(new CorpGroupAspectArray());
    String groupName = "bar";
    String member = "baz";
    String email = "xyz@linkedin.com";
    String admin1 = "admin1";
    String admin2 = "admin2";
    CorpuserUrn corpMember = new CorpuserUrn(member);
    CorpGroupUrn corpGroup = new CorpGroupUrn(groupName);
    CorpuserUrn corpAdmin1 = new CorpuserUrn(admin1);
    CorpuserUrn corpAdmin2 = new CorpuserUrn(admin2);
    CorpGroupInfo corpGroupInfo = new CorpGroupInfo().setAdmins(new CorpuserUrnArray(Arrays.asList(corpAdmin1, corpAdmin2))).setEmail(email)
        .setGroups(new CorpGroupUrnArray(Collections.singleton(corpGroup)))
        .setMembers(new CorpuserUrnArray(Collections.singleton(corpMember)));
    CorpGroupAspect corpGroupAspect = new CorpGroupAspect();
    corpGroupAspect.setCorpGroupInfo(corpGroupInfo);
    corpGroupSnapshot.getAspects().add(corpGroupAspect);

    List<CorpGroupDocument> actualDocs = new CorpGroupIndexBuilder().getDocumentsToUpdate(corpGroupSnapshot);

    assertEquals(actualDocs.size(), 1);
    assertEquals(actualDocs.get(0).getUrn(), corpGroupUrn);
    assertEquals(actualDocs.get(0).getAdmins(), Arrays.asList(admin1, admin2));
    assertEquals(actualDocs.get(0).getMembers(), Collections.singletonList(member));
    assertEquals(actualDocs.get(0).getGroups(), Collections.singletonList(groupName));
    assertEquals(actualDocs.get(0).getEmail(), email);
  }
}
