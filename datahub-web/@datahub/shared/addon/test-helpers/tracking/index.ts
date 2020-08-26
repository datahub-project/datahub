import { getPiwikActivityQueue } from '@datahub/shared/utils/tracking/piwik';

/**
 * Convenience function to get the activity queue
 * @returns {Array<Array<string>>}
 */
export const getQueue = (): Array<Array<string>> => getPiwikActivityQueue();

/**
 * Returns an iteratee to match an activityId string with a string found at the head position in a queue
 * @param {string} activityId
 * @returns {((arg: Array<string>) => boolean)}
 */
export const findInQueue = (activityId: string): ((arg: Array<string>) => boolean) => ([activityIdInQueue]: Array<
  string
>): boolean => activityIdInQueue === activityId;
