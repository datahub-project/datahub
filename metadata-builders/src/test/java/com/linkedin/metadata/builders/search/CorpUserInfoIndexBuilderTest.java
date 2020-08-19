package com.linkedin.metadata.builders.search;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.data.template.StringArray;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.aspect.CorpUserAspectArray;
import com.linkedin.metadata.search.CorpUserInfoDocument;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class CorpUserInfoIndexBuilderTest {

  @Test
  public void testGetDocumentsToUpdateFromCorpUserSnapshot() {
    String testerLdap = "fooBar";
    CorpuserUrn corpuserUrn = new CorpuserUrn(testerLdap);
    CorpUserSnapshot corpUserSnapshot =
        new CorpUserSnapshot().setUrn(corpuserUrn).setAspects(new CorpUserAspectArray());
    CorpUserInfo corpUserInfo = CorpUserInfoTestUtils.corpUserInfo();
    corpUserSnapshot.getAspects().add(CorpUserInfoTestUtils.corpUserAspect(corpUserInfo));
    List<CorpUserInfoDocument> actualDocs = new CorpUserInfoIndexBuilder().getDocumentsToUpdate(corpUserSnapshot);
    assertEquals(actualDocs.size(), 1);
    assertEquals(actualDocs.get(0).getUrn(), corpuserUrn);
    assertEquals(actualDocs.get(0).getTitle(), "fooBarEng");
    assertTrue(actualDocs.get(0).isActive());

    CorpUserEditableInfo corpUserEditableInfo1 = new CorpUserEditableInfo().setAboutMe("An Engineer")
        .setSkills(new StringArray(Arrays.asList("skill1", "skill2", "skill3")));
    corpUserSnapshot.getAspects().add(CorpUserInfoTestUtils.corpUserAspect(corpUserEditableInfo1));
    actualDocs = new CorpUserInfoIndexBuilder().getDocumentsToUpdate(corpUserSnapshot);
    assertEquals(actualDocs.size(), 2);
    assertEquals(actualDocs.get(1).getAboutMe(), "An Engineer");
    assertEquals(actualDocs.get(1).getTeams(), Collections.emptyList());
    assertEquals(actualDocs.get(1).getSkills(), Arrays.asList("skill1", "skill2", "skill3"));

    CorpUserEditableInfo corpUserEditableInfo2 = new CorpUserEditableInfo()
        .setTeams(new StringArray(Arrays.asList("team1", "team2")));
    corpUserSnapshot.getAspects().add(CorpUserInfoTestUtils.corpUserAspect(corpUserEditableInfo2));
    actualDocs = new CorpUserInfoIndexBuilder().getDocumentsToUpdate(corpUserSnapshot);
    assertEquals(actualDocs.size(), 3);
    assertEquals(actualDocs.get(2).getAboutMe(), "");
    assertEquals(actualDocs.get(2).getSkills(), Collections.emptyList());
    assertEquals(actualDocs.get(2).getTeams(), Arrays.asList("team1", "team2"));
  }
}
