declare namespace Com {
  namespace Linkedin {
    namespace Avro2pegasus {
      namespace Events {
        namespace Common {
          export interface AuditStamp {
            time: number;
            actor: string;
            impersonator?: string;
          }
        }
        namespace Datavault {
          export type PrincipalType = unknown;
        }
      }
    }

    namespace Common {
      export type Time = number;

      export type Url = string;

      export type Uri = string;

      export type EmailAddress = string;

      export interface VersionTag {
        versionTag?: string;
      }

      export interface Status {
        scn: number;
        entityId: number;
        entityType: number;
        statusId: number;
        seatId: number;
        createdDate: number;
        active: boolean;
        attributes: number;
        status: string;
        lastUpdated?: number;
        contractId?: number;
        parentStatusId?: number;
      }

      export type FabricType =
        | 'DEV'
        | 'EI'
        | 'PROD'
        | 'CORP'
        | 'LIT'
        | 'PRIME'
        | 'MANDA'
        | 'AZURECONTROL'
        | 'AZUREPROD'
        | 'AZUREEI'
        | 'CMD';

      export type OwnershipType =
        | 'DEVELOPER'
        | 'SUBJECT_MATTER_EXPERT'
        | 'DELEGATE'
        | 'PRODUCER'
        | 'CONSUMER'
        | 'STAKEHOLDER';

      export interface OwnershipSource {
        type:
          | 'AUDIT'
          | 'DATABASE'
          | 'FILE_SYSTEM'
          | 'ISSUE_TRACKING_SYSTEM'
          | 'MANUAL'
          | 'SERVICE'
          | 'SOURCE_CONTROL'
          | 'OTHER';
        url?: string;
      }

      export interface Owner {
        owner: string;
        type: Com.Linkedin.Common.OwnershipType;
        source?: Com.Linkedin.Common.OwnershipSource;
      }
      export interface Ownership {
        owners: Com.Linkedin.Common.Owner[];
        lastModified?: Com.Linkedin.Common.AuditStamp;
      }

      export interface OwnershipSuggestion {
        owners: Com.Linkedin.Common.Owner[];
      }

      export interface AuditStamp {
        time: Com.Linkedin.Common.Time;
        actor: string;
        impersonator?: string;
      }

      export interface LikeAction {
        likedBy: string;
        lastModified?: Com.Linkedin.Common.AuditStamp;
      }

      export interface Likes {
        actions: Com.Linkedin.Common.LikeAction[];
      }

      export type FollowerType = {
        corpUser?: string;
        corpGroup?: string;
      };

      export interface FollowAction {
        follower: Com.Linkedin.Common.FollowerType;
        lastModified?: Com.Linkedin.Common.AuditStamp;
      }

      export interface Follow {
        followers: Com.Linkedin.Common.FollowAction[];
      }

      export interface ChangeAuditStamps {
        created: Com.Linkedin.Avro2pegasus.Events.Common.AuditStamp;
        lastModified: Com.Linkedin.Avro2pegasus.Events.Common.AuditStamp;
        deleted?: Com.Linkedin.Avro2pegasus.Events.Common.AuditStamp;
      }
      export interface BaseFieldMapping {
        created?: Com.Linkedin.Common.AuditStamp;
        transformationFunction?: string;
      }

      export interface EntityTopUsage {
        mostFrequentUsers: Array<{
          identity: {
            gridUser?: string;
            corpUser?: string;
          };
          accessCount: number;
        }>;
        mostFrequentGroups: Array<{
          identity: {
            gridGroup?: string;
            corpGroup?: string;
          };
          accessCount: number;
        }>;
      }
      export interface HealthValidation {
        tier: 'CRITICAL' | 'WARNING' | 'MINOR';
        score: number;
        description: string;
        weight: number;
        validator: string;
      }

      export interface Health {
        score: number;
        validations: Com.Linkedin.Common.HealthValidation[];
        created?: Com.Linkedin.Common.AuditStamp;
      }

      export interface InstitutionalMemoryMetadata {
        url: Com.Linkedin.Common.Url;
        description: string;
        createStamp: Com.Linkedin.Common.AuditStamp;
      }

      export interface InstitutionalMemory {
        elements: Com.Linkedin.Common.InstitutionalMemoryMetadata[];
      }

      export type ValueSchemaLanguage =
        | 'AVRO'
        | 'MYSQL_DDL'
        | 'ORACLE_DDL'
        | 'ORC'
        | 'PARQUET'
        | 'PDL'
        | 'PDSC'
        | 'SCHEMALESS'
        | 'BINARY_JSON'
        | 'OTHER';

      export type KeySchemaLanguage = 'AVRO' | 'STRING' | 'OTHER';
    }

    namespace Schema {
      export type SchemaFieldDataType =
        | 'BOOLEAN'
        | 'BYTES'
        | 'ENUM'
        | 'NULL'
        | 'NUMBER'
        | 'STRING'
        | 'ARRAY'
        | 'MAP'
        | 'RECORD'
        | 'UNION';

      export interface SchemaField {
        fieldPath: Com.Linkedin.Dataset.SchemaFieldPath;
        pegasusFieldPath?: Com.Linkedin.Dataset.SchemaFieldPath;
        jsonPath?: string;
        nullable: boolean;
        description?: string;
        type: Com.Linkedin.Schema.SchemaFieldDataType;
        nativeDataType: string;
        recursive: boolean;
      }
      export interface NormalizedSchema {
        normalizedFields: Com.Linkedin.Schema.SchemaField[];
      }

      export interface ObservedSchemaDefinition {
        value: {
          schema?: string;
          schemaLanguage: Com.Linkedin.Common.ValueSchemaLanguage;
          registeredSchema?: string;
        };
        key?: {
          schema: string;
          schemaLanguage: Com.Linkedin.Common.KeySchemaLanguage;
          registeredSchema?: string;
        };
        normalizedSchema?: Com.Linkedin.Schema.NormalizedSchema;
      }
    }

    namespace Dataset {
      export type SchemaFieldPath = string;

      export interface DatasetKey {
        platform: string;
        name: string;
        origin: Com.Linkedin.Common.FabricType;
      }

      export interface DatasetProperties {
        description?: string;
        uri?: Com.Linkedin.Common.Uri;
        tags: string[];
        customProperties: { [id: string]: string };
      }

      export interface DeploymentInfo {
        dataLocation: {
          fabricGroup: Com.Linkedin.Common.FabricType;
          fabric?: string;
          cluster: string;
          region?: string;
        };
        additionalDeploymentInfo?: { [id: string]: string };
      }

      export interface DatasetDeprecation {
        deprecated: boolean;
        decommissionTime?: number;
        note: string;
        actor?: string;
      }

      export interface DatasetRefresh {
        lastRefresh: number;
      }

      export interface DatasetSchemaLineage {
        fieldMappings: Com.Linkedin.Dataset.DatasetSchemaFieldMapping[];
      }

      export interface DatasetSchemaFieldMapping extends Com.Linkedin.Common.BaseFieldMapping {
        sourceFields: string[];
        destinationField: string;
      }

      export interface DatasetUpstreamLineage {
        fieldMappings: Com.Linkedin.Dataset.DatasetFieldMapping[];
      }

      export type DatasetFieldUpstream = {
        'com.linkedin.common.DatasetFieldUrn': string;
      };

      export interface DatasetFieldMapping extends Com.Linkedin.Common.BaseFieldMapping {
        sourceFields: Com.Linkedin.Dataset.DatasetFieldUpstream[];
        destinationField: string;
      }

      export type DatasetLineageType = 'COPY' | 'TRANSFORMED' | 'VIEW' | 'MANAGE' | 'OBFUSCATED_COPY';

      export interface Upstream {
        auditStamp: Com.Linkedin.Common.AuditStamp;
        dataset: string;
        type: Com.Linkedin.Dataset.DatasetLineageType;
      }

      export interface UpstreamLineage {
        upstreams: Com.Linkedin.Dataset.Upstream[];
      }

      export interface Dataset
        extends Com.Linkedin.Dataset.DatasetKey,
          Com.Linkedin.Common.ChangeAuditStamps,
          Com.Linkedin.Common.VersionTag {
        id: number;
        deploymentInfos: Com.Linkedin.Dataset.DeploymentInfo[];
        description: string;
        removed: boolean;
        deprecation?: Com.Linkedin.Dataset.DatasetDeprecation;
        refresh?: Com.Linkedin.Dataset.DatasetRefresh;
        datasetSchemaLineage?: Com.Linkedin.Dataset.DatasetSchemaLineage;
        datasetUpstreamLineage?: Com.Linkedin.Dataset.DatasetUpstreamLineage;
        entityTopUsage?: Com.Linkedin.Common.EntityTopUsage;
        follow?: Com.Linkedin.Common.Follow;
        health?: Com.Linkedin.Common.Health;
        institutionalMemory?: Com.Linkedin.Common.InstitutionalMemory;
        likes?: Com.Linkedin.Common.Likes;
        observedSchemaDefinition?: Com.Linkedin.Schema.ObservedSchemaDefinition;
        ownership?: Com.Linkedin.Common.Ownership;
        ownershipSuggestion?: Com.Linkedin.Common.OwnershipSuggestion;
        upstreamLineage?: Com.Linkedin.Dataset.UpstreamLineage;
        status?: Com.Linkedin.Common.Status;
        platformNativeType?: 'TABLE' | 'VIEW' | 'DIRECTORY' | 'STREAM' | 'BUCKET';
        uri?: Com.Linkedin.Common.Uri;
        tags: string[];
        properties?: { [id: string]: string };
      }

      export type OwnershipProvider = unknown;

      export type OwnerCategory = 'DATA_OWNER' | 'PRODUCER' | 'DELEGATE' | 'STAKEHOLDER' | 'CONSUMER';

      export interface SchemaFieldPaths {
        fieldPath: Com.Linkedin.Dataset.SchemaFieldPath;
        pegasusFieldPath: Com.Linkedin.Dataset.SchemaFieldPath;
      }

      export type ComplianceDataType = string;
      export type FieldFormat = string;
      export type SecurityClassification = string;

      export interface FieldCompliance extends Com.Linkedin.Dataset.SchemaFieldPaths {
        dataType: Com.Linkedin.Dataset.ComplianceDataType;
        fieldFormat?: Com.Linkedin.Dataset.FieldFormat;
        valuePattern?: string;
        nonOwner?: boolean;
        purgeKey?: boolean;
        securityClassification?: Com.Linkedin.Dataset.SecurityClassification;
        providedByUser?: boolean;
        containingPersonalData?: boolean;
        readonly?: boolean;
      }
    }
    namespace DataConstructChangeManagement {
      export type Category =
        | 'BUSINESS_LOGIC'
        | 'DEPRECATION'
        | 'LINEAGE'
        | 'REMOVAL'
        | 'SCHEDULING'
        | 'SCHEMA_UPDATE'
        | 'OTHER';

      export type State = 'DEPLOYED' | 'DRAFT' | 'IMPLEMENTING' | 'PROPOSED' | 'WITHDRAWN';

      export type NotificationRecipient = {
        groupUrn?: string;
        userUrn?: string;
      };

      export type OwningEntity = {
        dataset?: string;
      };

      export interface NotificationTypes {
        jira: boolean;
        email: boolean;
        banner: boolean;
      }

      export interface Message {
        subject: string;
        messageText: string;
        documentationLink?: Com.Linkedin.Common.Url;
      }
      export interface Notification {
        recipients: Com.Linkedin.DataConstructChangeManagement.NotificationRecipient[];
        notificationTypes: Com.Linkedin.DataConstructChangeManagement.NotificationTypes;
        publishTimeAt?: Com.Linkedin.Common.Time;
      }

      export interface DataConstructChangeManagementContent {
        owningEntity: Com.Linkedin.DataConstructChangeManagement.OwningEntity;
        category: Com.Linkedin.DataConstructChangeManagement.Category;
        state?: Com.Linkedin.DataConstructChangeManagement.State;
        message: Com.Linkedin.DataConstructChangeManagement.Message;
        notification?: Com.Linkedin.DataConstructChangeManagement.Notification;
        lastModified: Com.Linkedin.Common.AuditStamp;
      }

      export interface DataConstructChangeManagement
        extends Com.Linkedin.DataConstructChangeManagement.DataConstructChangeManagementContent {
        id: number;
      }
    }
    namespace Metadata {
      namespace Graph {
        // Generated from: com/linkedin/metadata/graph/Attribute.pdsc

        export interface Attribute {
          name: string;
          type?: string;
          reference?: Com.Linkedin.Metadata.Graph.NodeId;
          value?: string;
        }
        // Generated from: com/linkedin/metadata/graph/Edge.pdsc

        export interface Edge {
          attributes?: Com.Linkedin.Metadata.Graph.Attribute[];
          fromNode: Com.Linkedin.Metadata.Graph.NodeId;
          fromAttribute?: string;
          toNode: Com.Linkedin.Metadata.Graph.NodeId;
          toAttribute?: string;
        }
        // Generated from: com/linkedin/metadata/graph/Graph.pdsc

        export interface Graph {
          rootNode?: Com.Linkedin.Metadata.Graph.NodeId;
          nodes: Com.Linkedin.Metadata.Graph.Node[];
          edges?: Com.Linkedin.Metadata.Graph.Edge[];
        }
        // Generated from: com/linkedin/metadata/graph/Node.pdsc

        export interface Node {
          id: Com.Linkedin.Metadata.Graph.NodeId;
          entityUrn?: string;
          displayName?: string;
          referencedSchema?: string;
          attributes?: Com.Linkedin.Metadata.Graph.Attribute[];
        }
        // Generated from: com/linkedin/metadata/graph/NodeId.pdsc

        export type NodeId = string;
      }

      namespace Aspect {
        export type DatasetAspect = {
          'com.linkedin.common.Ownership'?: Com.Linkedin.Common.Ownership;
          'com.linkedin.common.Status'?: Com.Linkedin.Common.Status;
          'com.linkedin.common.Follow'?: Com.Linkedin.Common.Follow;
          'com.linkedin.common.Health'?: Com.Linkedin.Common.Health;
          'com.linkedin.common.InstitutionalMemory'?: Com.Linkedin.Common.InstitutionalMemory;
          'com.linkedin.common.Likes'?: Com.Linkedin.Common.Likes;
          'com.linkedin.common.EntityTopUsage'?: Com.Linkedin.Common.EntityTopUsage;
        };

        export type DataConstructChangeManagementAspect = {
          'com.linkedin.dataConstructChangeManagement.DataConstructChangeManagementContent': Com.Linkedin.DataConstructChangeManagement.DataConstructChangeManagementContent;
        };

        export type CorpUserAspect = {
          'com.linkedin.identity.CorpUserInfo'?: Com.Linkedin.Identity.CorpUserInfo;
          'com.linkedin.identity.CorpUserEditableInfo'?: Com.Linkedin.Identity.CorpUserEditableInfo;
          'com.linkedin.identity.DatasetRecommendationsInfo'?: Com.Linkedin.Identity.DatasetRecommendationsInfo;
        };
      }

      namespace Snapshot {
        export interface DatasetSnapshot {
          urn: string;
          aspects: Com.Linkedin.Metadata.Aspect.DatasetAspect[];
        }
      }
    }

    namespace Identity {
      export interface CorpGroupInfo {
        email: Com.Linkedin.Common.EmailAddress;
        admins: string[];
        members: string[];
        groups: string[];
      }

      export interface CorpUserInfo {
        active: boolean;
        displayName?: string;
        email: Com.Linkedin.Common.EmailAddress;
        title?: string;
        managerUrn?: string;
        departmentId?: number;
        departmentName?: string;
        firstName?: string;
        lastName?: string;
        fullName?: string;
        countryCode?: string;
      }
      export interface CorpUserEditableInfo {
        aboutMe?: string;
        teams: string[];
        skills: string[];
      }
      export interface DatasetRecommendationsInfo {
        userActivityRecommendations: Com.Linkedin.Identity.DatasetRecommendation[];
        peerActivityRecommendations: Com.Linkedin.Identity.DatasetRecommendation[];
      }
      export interface DatasetRecommendation extends Com.Linkedin.Identity.BaseRecommendation {
        datasetUrn: string;
      }

      export interface BaseRecommendation {
        confidence: number;
        reason: string;
      }
    }
    namespace Metric {
      export type MetricFrequencyType = 'REALTIME' | 'HOURLY' | 'DAILY' | 'WEEKLY' | 'MONTHLY';
    }
  }
}
