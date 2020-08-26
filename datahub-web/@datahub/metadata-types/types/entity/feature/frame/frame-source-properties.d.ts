import { IEspressoSourceProperties } from '@datahub/metadata-types/types/entity/feature/frame/espresso-source-properties';
import { IHDFSSourceProperties } from '@datahub/metadata-types/types/entity/feature/frame/hdfs-source-properties';
import { IVoldemortSourceProperties } from '@datahub/metadata-types/types/entity/feature/frame/voldermort-source-properties';
import { IRestliSourceProperties } from '@datahub/metadata-types/types/entity/feature/frame/restli-source-properties';
import { IVeniceSourceProperties } from '@datahub/metadata-types/types/entity/feature/frame/venice-source-properties';
import { IPassthroughSourceProperties } from '@datahub/metadata-types/types/entity/feature/frame/passthrough-source-properties';

/**
 * A union of all supported source property types
 */
interface IFrameSourceProperties {
  'com.linkedin.feature.frame.EspressoSourceProperties'?: IEspressoSourceProperties;
  'com.linkedin.feature.frame.HDFSSourceProperties'?: IHDFSSourceProperties;
  'com.linkedin.feature.frame.VoldemortSourceProperties'?: IVoldemortSourceProperties;
  'com.linkedin.feature.frame.RestliSourceProperties'?: IRestliSourceProperties;
  'com.linkedin.feature.frame.VeniceSourceProperties'?: IVeniceSourceProperties;
  'com.linkedin.feature.frame.PassthroughSourceProperties'?: IPassthroughSourceProperties;
}
