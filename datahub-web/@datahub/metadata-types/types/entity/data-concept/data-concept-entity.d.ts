import { InstitutionalMemories } from '@datahub/metadata-types/types/aspects/institutional-memory';
import { IAuditStamp } from '@datahub/metadata-types/types/common/audit-stamp';

/**
 * An object containing the base properties for a related entity to a data concept. An entity is
 * arbitrarily determined to be related when it contains some data that describes the concept
 * Built from ref:
 * https://jarvis.corp.linkedin.com/codesearch/result/?path=metadata-models%2Fmetadata-models%2Fsrc%2Fmain%2Fpegasus%2Fcom%2Flinkedin%2FdataConcept&reponame=multiproducts%2Fmetadata-models&name=RelatedEntities.pdsc
 */
export interface IDataConceptRelatedEntity {
  // Describes how the entity is related to the data concept
  description: string;
}

/**
 * A reference to the dataset that is related to the data concept
 */
interface IDataConeptRelatedDataset extends IDataConceptRelatedEntity {
  datasetUrn: string;
}

/**
 * A reference to the metric that is related to the data concept
 */
interface IDataConeptRelatedMetric extends IDataConceptRelatedEntity {
  metricUrn: string;
}

/**
 * A reference to the feature that is related to the data concept
 */
interface IDataConceptRelatedFeature extends IDataConceptRelatedEntity {
  featureUrn: string;
}

/**
 * A reference to the related inchart that is related to the data concept
 */
interface IDataConceptRelatedChart extends IDataConceptRelatedEntity {
  inchartsDashboardUrn: string;
}

/**
 * Reference to related entities to the data concept in some way
 * Reference:
 * https://jarvis.corp.linkedin.com/codesearch/result/?path=metadata-models%2Fmetadata-models%2Fsrc%2Fmain%2Fpegasus%2Fcom%2Flinkedin%2FdataConcept&reponame=multiproducts%2Fmetadata-models&name=RelatedEntities.pdsc
 */
interface IDataConceptRelatedEntities {
  createStamp: IAuditStamp;
  relatedDatasets: Array<IDataConeptRelatedDataset>;
  relatedMetrics: Array<IDataConeptRelatedMetric>;
  relatedFeatures: Array<IDataConceptRelatedFeature>;
  relatedInchartsDashboards: Array<IDataConceptRelatedChart>;
}

/**
 * Descriptive properties related to a data concept object
 * Reference:
 * https://jarvis.corp.linkedin.com/codesearch/result/?path=metadata-models%2Fmetadata-models%2Fsrc%2Fmain%2Fpegasus%2Fcom%2Flinkedin%2FdataConcept&reponame=multiproducts%2Fmetadata-models&name=DataConceptProperties.pdsc
 */
export interface IDataConceptProperties {
  // Name of the related data concept
  name: string;
  // Tags identifying useful properties for the data concept
  tags: Array<string>;
  lastUpdatedTimeStamp: IAuditStamp;
  // Provided description for the related data concept, regarding its definition and how it is used
  description: string;
}

/**
 * A data concept is a human created construct that describes some data driven idea at the company
 * Reference:
 * https://rb.corp.linkedin.com/r/1821269
 */
export interface IDataConcept {
  // Identifier for the data concept
  conceptId: number;
  // Intitutional memory, i.e. corpuser provided content related to a data concept
  institutionalMemory?: { elements: InstitutionalMemories };
  // Details related to owners for the data concept
  ownership?: Com.Linkedin.Common.Ownership;
  // Object containing the properties related to this data concept
  dataConceptProperties?: IDataConceptProperties;
  // A map to entities that are related to the data concept
  relatedEntities?: IDataConceptRelatedEntities;
}
