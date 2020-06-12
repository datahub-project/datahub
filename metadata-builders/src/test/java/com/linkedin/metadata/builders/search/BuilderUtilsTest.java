package com.linkedin.metadata.builders.search;

import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.FabricType;
import com.linkedin.common.MultiLocaleString;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.data.template.StringMap;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class BuilderUtilsTest {

  @Test
  public void testGetCorpUserOwners() {
    Ownership ownership = new Ownership();
    Owner owner1 = new Owner().setOwner(new CorpuserUrn("t1"));
    Owner owner2 = new Owner().setOwner(new CorpuserUrn("t2"));
    Owner owner3 = new Owner().setOwner(new CorpGroupUrn("t3"));
    List<Owner> owners = Arrays.asList(owner1, owner2, owner3);
    ownership.setOwners(new OwnerArray(owners));
    assertEquals(BuilderUtils.getCorpUserOwners(ownership), Arrays.asList("t1", "t2"));
  }

  @Test
  public void testGetDatasetNames() {
    DatasetUrnArray datasetUrns = new DatasetUrnArray();
    datasetUrns.add(new DatasetUrn(new DataPlatformUrn("hdfs"), "testDataset1", FabricType.EI));
    datasetUrns.add(new DatasetUrn(new DataPlatformUrn("hdfs"), "testDataset2", FabricType.EI));
    assertEquals(BuilderUtils.getDatasetNames(datasetUrns), Arrays.asList("testDataset1", "testDataset2"));
  }

  @Test
  public void testMultiLocale() {

    String enUSLocale = "en_US";
    String enINLocale = "en_IN";
    String deDELocale = "de_DE";

    String enUSStr = "English USA";
    String enINStr = "English India";
    String deDEStr = "German String";

    MultiLocaleString multiLocaleString =  new MultiLocaleString();
    StringMap stringMap = new StringMap();
    multiLocaleString.setLocalized(stringMap);
    assertEquals(BuilderUtils.convertMultiLocaleStringToString(multiLocaleString, enUSLocale), "");

    stringMap.put(enUSLocale, enUSStr);
    multiLocaleString.setLocalized(stringMap);
    assertEquals(BuilderUtils.convertMultiLocaleStringToString(multiLocaleString, enUSLocale), enUSStr);

    MultiLocaleString multiLocaleString1 =  new MultiLocaleString();
    StringMap stringMap1 = new StringMap();
    stringMap1.put(enINLocale, enINStr);
    multiLocaleString1.setLocalized(stringMap1);
    assertEquals(BuilderUtils.convertMultiLocaleStringToString(multiLocaleString1, enUSLocale), enINStr);

    MultiLocaleString multiLocaleString2 =  new MultiLocaleString();
    StringMap stringMap2 = new StringMap();
    stringMap2.put(enINLocale, enINStr);
    stringMap2.put(deDELocale, deDEStr);
    stringMap2.put(enUSLocale, enUSStr);
    multiLocaleString2.setLocalized(stringMap2);
    assertEquals(BuilderUtils.convertMultiLocaleStringToString(multiLocaleString2, enUSLocale), enUSStr);
  }
}
