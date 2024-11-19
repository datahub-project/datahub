package com.datahub.iceberg.catalog;

import static com.linkedin.metadata.Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATA_PLATFORM_INSTANCE_ENTITY_NAME;
import static com.linkedin.metadata.utils.GenericRecordUtils.serializeAspect;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DataPlatformInstanceKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;
import lombok.SneakyThrows;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;

public class Utils {
  private static final String PLATFORM_NAME = "nativeIceberg";

  public static AuditStamp auditStamp() {
    try {
      return new AuditStamp()
          .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
          .setTime(System.currentTimeMillis());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public static MetadataChangeProposal platformInstanceMcp(
      String platformInstanceName, Urn urn, String entityType) {
    DataPlatformInstance platformInstance = new DataPlatformInstance();
    platformInstance.setPlatform(platformUrn());
    platformInstance.setInstance(platformInstanceUrn(platformInstanceName));

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(urn);
    mcp.setEntityType(entityType);
    mcp.setAspectName(DATA_PLATFORM_INSTANCE_ASPECT_NAME);
    mcp.setAspect(serializeAspect(platformInstance));
    mcp.setChangeType(ChangeType.UPSERT);

    return mcp;
  }

  public static DataPlatformUrn platformUrn() {
    return new DataPlatformUrn(PLATFORM_NAME);
  }

  public static Urn platformInstanceUrn(String platformInstance) {
    DataPlatformInstanceKey platformInstanceKey =
        new DataPlatformInstanceKey().setInstance(platformInstance).setPlatform(platformUrn());
    return EntityKeyUtils.convertEntityKeyToUrn(
        platformInstanceKey, DATA_PLATFORM_INSTANCE_ENTITY_NAME);
  }

  public static FabricType fabricType() {
    // TODO configurable fabricType
    return FabricType.DEV;
  }

  public static Urn containerUrn(String platformInstance, Namespace ns) {
    return containerUrn(platformInstance, ns.levels());
  }

  @SneakyThrows
  public static Urn containerUrn(String platformInstance, String[] levels) {
    StringBuilder containerFullName = new StringBuilder(platformInstance);
    for (String level : levels) {
      containerFullName.append(".").append(level);
    }
    return Urn.createFromString("urn:li:container:nativeIceberg__" + containerFullName);
  }

  public static DatasetUrn datasetUrn(String platformInstance, TableIdentifier tableIdentifier) {
    return new DatasetUrn(
        platformUrn(), CatalogUtil.fullTableName(platformInstance, tableIdentifier), fabricType());
  }

  public static String fullTableName(String platformInstance, TableIdentifier tableIdentifier) {
    return CatalogUtil.fullTableName(platformInstance, tableIdentifier);
  }

  public static Set<String> locations(TableMetadata tableMetadata) {
    Set<String> locations = new HashSet<>();
    locations.add(tableMetadata.location());
    if (tableMetadata.properties().containsKey(TableProperties.WRITE_DATA_LOCATION)) {
      locations.add(tableMetadata.properties().get(TableProperties.WRITE_DATA_LOCATION));
    }
    if (tableMetadata.properties().containsKey(TableProperties.WRITE_METADATA_LOCATION)) {
      locations.add(tableMetadata.properties().get(TableProperties.WRITE_METADATA_LOCATION));
    }
    return locations;
  }

  public static Namespace namespaceFromString(String namespace) {
    return RESTUtil.decodeNamespace(URLEncoder.encode(namespace, Charset.defaultCharset()));
  }

  public static TableIdentifier tableIdFromString(String namespace, String table) {
    return TableIdentifier.of(namespaceFromString(namespace), RESTUtil.decodeString(table));
  }

  public static String parentDir(String fileLocation) {
    return fileLocation.substring(0, fileLocation.lastIndexOf("/"));
  }
}
