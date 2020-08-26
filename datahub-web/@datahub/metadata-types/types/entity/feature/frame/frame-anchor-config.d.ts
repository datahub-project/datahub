import { IFrameSourceConfig } from '@datahub/metadata-types/types/entity/feature/frame/frame-source-config';
import { IAvailabilityInfo } from '@datahub/metadata-types/types/entity/feature/frame/availability-info';

/**
 * Represents a anchor definition as defined in the frame feature config
 * @export
 * @interface IFrameAnchorConfig
 */
export interface IFrameAnchorConfig {
  // Name of the anchor
  name: string;
  // Source associated with this anchor
  source: IFrameSourceConfig;
  // Key expression for the anchor
  key?: string | Array<string>;
  // Fully qualified extractor class name
  extractor?: string;
  // Environment and fabric where anchor is defined, e.g. in ONLINE EI fabric or PROD
  availability: IAvailabilityInfo;
}
