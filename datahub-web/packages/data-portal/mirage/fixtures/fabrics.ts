import { FabricType } from '@datahub/metadata-types/constants/common/fabric-type';

export default Object.values(FabricType).map(fabric => ({
  displayTitle: fabric,
  origin: fabric
}));
