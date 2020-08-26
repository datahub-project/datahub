import { IFrameAnchorConfig } from '@datahub/metadata-types/types/entity/feature/frame/frame-anchor-config';
import { IFrameFeatureEditableConfig } from '@datahub/metadata-types/types/entity/feature/frame/frame-feature-editable-config';
import { FeatureEntity } from '@datahub/data-models/entity/feature/feature-entity';
import { IFeatureGroup, FeatureConfigDictionary } from '@datahub/data-models/types/entity/rendering/text-render-props';
import { FrameSourceType } from '@datahub/metadata-types/constants/entity/feature/frame/frame-source-type';
import { groupBy, uniqBy, flatten } from 'lodash';
import { hdfsSourceAttribute, offlineFeatureSourceUrlBase } from '@datahub/data-models/constants/entity/feature/source';
import { IFrameSourceConfig } from '@datahub/metadata-types/types/entity/feature/frame/frame-source-config';
import { getProperties } from '@ember/object';
/**
 * String literal of aspect properties to be rendered, used in the lookup table below, containing a map of keys to display names
 */
export type IFeatureRenderedAttributes =
  | 'description'
  | 'baseEntity'
  | 'classification'
  | 'inferType'
  | 'category'
  | 'documentationLink'
  | 'tier'
  | 'frameMp'
  | 'owners'
  | 'availability';

/**
 * Display names for attributes to be rendered
 */
export const attributeDisplayName: Record<IFeatureRenderedAttributes, string> = Object.freeze({
  description: 'Description',
  inferType: 'Fact or Inferred',
  baseEntity: ' Base Entity',
  classification: 'Classification',
  category: 'Catalog Hierarchy',
  documentationLink: 'Documentation Link',
  tier: 'Tier',
  frameMp: 'Frame MP',
  owners: 'Frame MP Owners',
  availability: 'Data Availability'
});

/**
 * Produces an array of strings depicting the hierarchy of a Feature Entity
 * TODO META-9971: The current hierarchy should be read from server
 */
const getHierarchySegments = ({ editableConfig, entity }: FeatureEntity): Array<string> => {
  if (editableConfig && entity) {
    const { name = '' } = entity;
    const {
      baseEntity = { baseEntityString: '' },
      classification = { classificationString: '' },
      category = { categoryString: '' }
    } = getProperties(editableConfig, ['baseEntity', 'category', 'classification']);

    const { baseEntityString } = baseEntity;
    const { classificationString } = classification;
    const { categoryString } = category;

    // Browse API uses '.' as a separator too so we have to split each part. Also
    // it needs to be lowercased.
    return flatten(
      [baseEntityString, classificationString, categoryString, name]
        .filter(Boolean)
        .map((path): Array<string> => path.toLowerCase().split('.'))
    );
  }

  return [];
};

/**
 * Reads the segments in a Feature hierarchy and generates a string representation
 * TODO META-9971: The current hierarchy should be read from server
 */
export const getCategoryPathString = (feature: FeatureEntity): string => {
  const hierarchySegments = getHierarchySegments(feature);
  return hierarchySegments.length > 1 ? hierarchySegments.slice(0, hierarchySegments.length - 1).join(' > ') : '-';
};

/**
 * Extracts the hdfs data source url from a frame source config
 */
const getFeatureFrameHdfsUrl = ({ properties, type }: IFrameSourceConfig): string => {
  const hdfsSourceProperties = properties[hdfsSourceAttribute];
  const hdfsSourcePathSegment = type === FrameSourceType.Hdfs && hdfsSourceProperties ? hdfsSourceProperties.path : '';
  return hdfsSourcePathSegment ? `${offlineFeatureSourceUrlBase}${hdfsSourcePathSegment}` : '';
};

/**
 * Creates a lookup table for each data source urn to its source path
 * { source urn => path to source }
 */
export const getFeatureDataSourceEntries = ({ featureConfigs }: FeatureEntity): Record<string, string> => {
  const defaultAnchors: Array<IFrameAnchorConfig> = [];
  const { featureConfig = { anchors: defaultAnchors } } = featureConfigs;
  const { anchors } = featureConfig;

  return anchors.reduce((previousEntries, { source }): Record<string, string> => {
    const { datasetUrn = '' } = source;

    return { ...previousEntries, [datasetUrn]: getFeatureFrameHdfsUrl(source) };
  }, {});
};

/**
 * Creates a mapping of top level editableConfig attributes to dot notation source paths, to enable future lookup of
 * source properties when performing an operation on the original source representation
 */
export const editableConfigRenderAttrToSourcePath: Required<{ [K in keyof IFrameFeatureEditableConfig]: string }> = {
  description: 'description',
  baseEntity: 'baseEntity.baseEntityString',
  classification: 'classification.classificationString',
  category: 'category.categoryString',
  inferType: 'inferType',
  documentationLink: 'documentationLink'
};

/**
 * Reads relevant properties on the Feature instance(s) and produces an mapping of replacement/substitutions
 * to be interpolated in a Tagged Template
 */
export const substitutionDictionary = (features: FeatureEntity | Array<FeatureEntity>): FeatureConfigDictionary => {
  const propertyNotFound = '**PROPERTY-NOTFOUND**';
  // Make into single array if a single Feature or an array Features is presented
  const featureList: Array<FeatureEntity> = [].concat.apply(features);
  // Enumerate the Feature dictionary names as indexable keys on the substitution dictionary
  const dictionaryKeys: Array<keyof IFeatureGroup> = ['frameMpName', 'frameMpVersion', 'featureName', 'listOfSources'];

  const definitions: Array<IFeatureGroup> = featureList.map(
    ({ multiproductInfo, hdfsPaths = [], entity }): IFeatureGroup => {
      const { version = propertyNotFound, name = propertyNotFound } = multiproductInfo || {};
      const featureName = entity && entity.name ? entity.name : propertyNotFound;

      return {
        featureName,
        frameMpName: name,
        frameMpVersion: version,
        listOfSources: hdfsPaths
      };
    }
  );

  const emptyDictionary: Partial<FeatureConfigDictionary> = {};
  // Group Features by dictionary keys
  const dictionaryWithoutFrameMpVersionAggregation = dictionaryKeys.reduce(
    (dictionary, dictionaryKey): Partial<FeatureConfigDictionary> => ({
      ...dictionary,
      [dictionaryKey]: groupBy(definitions, dictionaryKey)
    }),
    emptyDictionary
  ) as Pick<FeatureConfigDictionary, Exclude<keyof FeatureConfigDictionary, 'frameMpWithVersion'>>;

  // Aggregate the frameMp group by unique frame versions
  const frameMpWithVersion = flatten(
    Object.values(dictionaryWithoutFrameMpVersionAggregation.frameMpName).map(
      (frameMp): Array<IFeatureGroup> => uniqBy(frameMp, 'frameMpVersion')
    )
  );

  return { ...dictionaryWithoutFrameMpVersionAggregation, frameMpWithVersion };
};
