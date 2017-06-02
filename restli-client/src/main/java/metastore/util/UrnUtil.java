/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package metastore.util;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.MemberUrn;
import com.linkedin.common.urn.TupleKey;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataset.BytesType;
import com.linkedin.dataset.NullType;
import com.linkedin.dataset.NumberType;
import com.linkedin.dataset.PlatformNativeType;
import com.linkedin.dataset.SchemaFieldDataType;
import com.linkedin.dataset.SecurityClassification;
import com.linkedin.dataset.StringType;
import java.net.URISyntaxException;
import java.util.Collection;


public class UrnUtil {

  /**
   * Transform platform String into DataPlatformUrn
   * @param platformName String
   * @return DataPlatformUrn
   * @throws URISyntaxException
   */
  public static DataPlatformUrn toDataPlatformUrn(String platformName) throws URISyntaxException {
    return DataPlatformUrn.deserialize("urn:li:dataPlatform:" + platformName);
  }

  /**
   * Transform platform + dataset + origin into DatasetUrn
   * @param platformName String
   * @param datasetName String
   * @param origin String
   * @return DatasetUrn
   * @throws URISyntaxException
   */
  public static DatasetUrn toDatasetUrn(String platformName, String datasetName, String origin)
      throws URISyntaxException {
    TupleKey keys = new TupleKey("urn:li:dataPlatform:" + platformName, datasetName, toFabricType(origin).name());
    Urn urn = new Urn("dataset", keys);

    return DatasetUrn.createFromUrn(urn);
  }

  /**
   * Transform fabric String to FabricType
   * @param fabric String
   * @return FabricType
   */
  public static FabricType toFabricType(String fabric) {
    switch (fabric.toUpperCase()) {
      case "PROD":
        return FabricType.PROD;
      case "CORP":
        return FabricType.CORP;
      case "EI":
        return FabricType.EI;
      case "DEV":
        return FabricType.DEV;
      default:
        return FabricType.$UNKNOWN;
    }
  }

  /**
   * Transform nativeType String to PlatformNativeType enum
   * @param nativeType String
   * @return PlatformNativeType
   */
  public static PlatformNativeType toPlatformNativeType(String nativeType) {
    switch (nativeType.toUpperCase()) {
      case "TABLE":
        return PlatformNativeType.TABLE;
      case "VIEW":
        return PlatformNativeType.VIEW;
      case "STREAM":
        return PlatformNativeType.STREAM;
      case "DIRECTORY":
        return PlatformNativeType.DIRECTORY;
      default:
        return PlatformNativeType.$UNKNOWN;
    }
  }

  /**
   * Transform security classification string to SecurityClassification
   * @param securityClass String
   * @return SecurityClassification
   */
  public static SecurityClassification toSecurityClassification(String securityClass) {
    switch (securityClass.toUpperCase()) {
      case "HIGHLY_CONFIDENTIAL":
        return SecurityClassification.HIGHLY_CONFIDENTIAL;
      case "CONFIDENTIAL":
        return SecurityClassification.CONFIDENTIAL;
      case "UNCLASSIFIED":
      case "NOT CONFIDENTIAL":
        return SecurityClassification.UNCLASSIFIED;
      default:
        return SecurityClassification.$UNKNOWN;
    }
  }

  /**
   * Find the highest security classification from classification Strings
   * @param classifications Collection<String>
   * @return SecurityClassification
   */
  public static SecurityClassification getHighestSecurityClassification(Collection<String> classifications) {
    if (classifications.stream().anyMatch(s -> s.equalsIgnoreCase("HIGHLY_CONFIDENTIAL"))) {
      return SecurityClassification.HIGHLY_CONFIDENTIAL;
    } else if (classifications.stream().anyMatch(s -> s.equalsIgnoreCase("CONFIDENTIAL"))) {
      return SecurityClassification.CONFIDENTIAL;
    } else if (classifications.stream()
        .anyMatch(s -> s.equalsIgnoreCase("UNCLASSIFIED") || s.equalsIgnoreCase("NOT CONFIDENTIAL"))) {
      return SecurityClassification.UNCLASSIFIED;
    } else {
      return SecurityClassification.$UNKNOWN;
    }
  }

  /**
   * Convert data field native type string to SchemaFieldDataType
   * @param type String
   * @return SchemaFieldDataType
   */
  public static SchemaFieldDataType toFieldDataType(String type) {
    if (type == null) {
      return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NullType()));
    } else if (type.equalsIgnoreCase("NUMBER") || type.equalsIgnoreCase("LONG") || type.equalsIgnoreCase("FLOAT")) {
      return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType()));
    } else if (type.equalsIgnoreCase("CHAR") || type.equalsIgnoreCase("VARCHAR2") || type.equalsIgnoreCase("NVARCHAR2")
        || type.equalsIgnoreCase("XMLTYPE")) {
      return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType()));
    } else if (type.equalsIgnoreCase("DATE") || type.startsWith("TIMESTAMP")) {
      return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new NumberType()));
    } else if (type.equalsIgnoreCase("BLOB") || type.startsWith("CLOB") || type.equalsIgnoreCase("NCLOB")
        || type.equalsIgnoreCase("RAW")) {
      return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new BytesType()));
    } else {
      return new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType()));
    }
  }

  /**
   * Convert long memberId to MemberUrn
   * @param memberId long
   * @return MemberUrn
   * @throws URISyntaxException
   */
  public static MemberUrn toMemberUrn(long memberId) throws URISyntaxException {
    return MemberUrn.deserialize("urn:li:member:" + memberId);
  }

  /**
   * Convert String corp userId to CorpuserUrn
   * @param userId String
   * @return CorpuserUrn
   * @throws URISyntaxException
   */
  public static CorpuserUrn toCorpuserUrn(String userId) throws URISyntaxException {
    return CorpuserUrn.deserialize("urn:li:corpuser:" + userId);
  }

  /**
   * Split WhereHows dataset URN into two parts: platform + dataset name
   * Also replace '/' with '.' in dataset name for Espresso, Oracle, Dalids and Hive
   * E.g. oracle:///abc/def > [oracle, abc.def]
   * @param urn String WhereHows dataset URN
   * @return String[] platform + dataset name
   */
  public static String[] splitWhUrn(String urn) {
    int index = urn.indexOf(":///");
    String fabric = urn.substring(0, index);
    String dataset = urn.substring(index + 4);

    if (fabric.equalsIgnoreCase("espresso") || fabric.equalsIgnoreCase("oracle")
        || fabric.equalsIgnoreCase("dalids") || fabric.equalsIgnoreCase("hive")) {
      dataset = dataset.replace("/", ".");
    }
    return new String[]{fabric, dataset};
  }
}
