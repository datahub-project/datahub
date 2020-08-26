import { FrameSourceType } from '@datahub/metadata-types/constants/entity/feature/frame/frame-source-type';
import { IFrameSourceProperties } from '@datahub/metadata-types/types/entity/feature/frame/frame-source-properties';

/**
 * Represents a source for features as defined in the frame config
 * @export
 * @interface IFrameSourceConfig
 */
export interface IFrameSourceConfig {
  // Name of the source
  name: string;
  // Supported source types in Frame
  type: FrameSourceType;
  // Dataset urn corresponding to the source. Some sources may not have datasetUrn like passthrough sources
  datasetUrn?: string;
  // Properties associated with the source
  properties: IFrameSourceProperties;
}
