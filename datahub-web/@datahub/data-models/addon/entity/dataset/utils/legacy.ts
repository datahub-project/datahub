import { IDatasetEntity } from '@datahub/metadata-types/types/entity/dataset/dataset-entity';
import { FabricType } from '@datahub/metadata-types/constants/common/fabric-type';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';
import { buildDatasetLiUrn } from '@datahub/data-models/entity/dataset/utils/urn';

/**
 * TODO META-11674
 *
 * Converts current dataset definition into the legacy one
 */
export function toLegacy(dataset: Com.Linkedin.Dataset.Dataset, urn?: string): IDatasetEntity {
  const ds = {
    createdTime: dataset.created.time || 0,
    decommissionTime: dataset.deprecation?.decommissionTime || null,
    deprecated: dataset.deprecation?.deprecated || null,
    deprecationNote: dataset.deprecation?.note || null,
    description: dataset.description || '',
    fabric: (dataset.origin as FabricType) || FabricType.CORP,
    nativeName: dataset.name,
    platform: (dataset.platform as DatasetPlatform) || DatasetPlatform.HDFS,
    properties: '',
    tags: [],
    removed: dataset.removed,
    nativeType: dataset.platformNativeType || '',
    modifiedTime: dataset.lastModified.time || 0,
    healthScore: dataset.health?.score || 0
  };
  const uri = urn || buildDatasetLiUrn(ds.platform, ds.nativeName, ds.fabric);
  return {
    ...ds,
    uri
  };
}

/**
 * TODO META-11674
 *
 * Converts legacy one dataset definition into the current
 */
export function fromLegacy(dataset: IDatasetEntity): Com.Linkedin.Dataset.Dataset {
  return {
    // Using -1 as we don't have that information in IDatasetEntity and to make sure it is an invalid id (negative)
    id: -1,
    name: dataset.nativeName,
    description: dataset.description,
    removed: dataset.removed,
    origin: dataset.fabric,
    platform: dataset.platform,
    created: {
      time: dataset.createdTime,
      actor: 'Not Available'
    },
    lastModified: {
      time: dataset.modifiedTime,
      actor: 'Not Available'
    },
    properties: {},
    tags: [],
    deploymentInfos: [],
    health: dataset.healthScore
      ? {
          score: dataset.healthScore,
          validations: []
        }
      : undefined,
    deprecation: dataset.deprecated
      ? {
          deprecated: dataset.deprecated,
          decommissionTime: dataset.decommissionTime || undefined,
          note: dataset.deprecationNote || ''
        }
      : undefined
  };
}
