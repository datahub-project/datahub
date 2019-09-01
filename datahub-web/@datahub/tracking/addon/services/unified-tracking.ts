import Service from '@ember/service';

/**
 * Defines the base and full api for the analytics / tracking module in Data Hub
 * @export
 * @class UnifiedTracking
 */
export default class UnifiedTracking extends Service {}

declare module '@ember/service' {
  // eslint-disable-next-line @typescript-eslint/interface-name-prefix
  interface Registry {
    'unified-tracking': UnifiedTracking;
  }
}
