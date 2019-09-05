import { Time } from '@datahub/metadata-types/types/common/time';

/**
 * The minimum number of Time for dwell time to be considered successful
 * ms
 * @type Time
 */
export const SuccessfulDwellTimeLength: Time = 5 * 1000;

/**
 * Defines the search route name
 * @type string
 */
export const searchRouteName = 'search';
