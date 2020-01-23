import { getDatasetUrnParts } from '@datahub/data-models/entity/dataset/utils/urn';
import { FabricType } from '@datahub/metadata-types/constants/common/fabric-type';

export const browsePaths = (_schema: any, request: any): Array<string> => {
  const {
    queryParams: { urn }
  } = request;

  const { platform, prefix = '', fabric = FabricType.PROD } = getDatasetUrnParts(urn);
  return [`${fabric}/${platform}/${prefix}`];
};
