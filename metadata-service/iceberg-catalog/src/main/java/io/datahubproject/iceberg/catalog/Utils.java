package io.datahubproject.iceberg.catalog;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.GenericRecordUtils.serializeAspect;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DataPlatformInstanceKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
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
  public static final String PLATFORM_NAME = "iceberg";

  private static final String NAMESPACE_CONTAINER_PREFIX = "urn:li:container:iceberg__";

  @SneakyThrows
  public static AuditStamp auditStamp() {
    return new AuditStamp()
        .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
        .setTime(System.currentTimeMillis());
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

  public static Urn containerUrn(String platformInstance, Namespace ns) {
    return containerUrn(platformInstance, ns.levels());
  }

  @SneakyThrows
  public static Urn containerUrn(String platformInstance, String[] levels) {
    StringBuilder containerFullName = new StringBuilder(platformInstance);
    for (String level : levels) {
      containerFullName.append(".").append(level);
    }
    return Urn.createFromString(NAMESPACE_CONTAINER_PREFIX + containerFullName);
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

  public static String namespaceNameFromContainerUrn(Urn urn) {
    return urn.toString().substring(NAMESPACE_CONTAINER_PREFIX.length());
  }
}
