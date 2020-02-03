import { IDatasetEntity } from '@datahub/metadata-types/types/entity/dataset/dataset-entity';
import { FabricType } from '@datahub/metadata-types/constants/common/fabric-type';
import { DatasetPlatform } from '@datahub/metadata-types/constants/entity/dataset/platform';

export const mockDatasetEntity = (overrideProps: Partial<IDatasetEntity> = {}): IDatasetEntity => ({
  uri: 'urn',
  createdTime: 8675309,
  decommissionTime: 8675309,
  deprecated: false,
  deprecationNote: '',
  description: '',
  fabric: FabricType.CORP,
  modifiedTime: 8675309,
  nativeName: 'CHAR_MANDER',
  nativeType: '',
  platform: DatasetPlatform.HDFS,
  properties: '',
  removed: false,
  tags: [],
  ...overrideProps
});
