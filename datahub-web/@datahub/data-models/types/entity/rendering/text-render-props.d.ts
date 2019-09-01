/**
 * Feature product spec configuration template substitution field types
 * @export
 * @interface IFeatureGroupValue
 */
export interface IFeatureGroup {
  frameMpName: string;
  frameMpVersion: string;
  featureName: string;
  listOfSources: Array<string>;
}

/**
 * Dictionary of each Feature group key with related definitions for config file download and aggregation for
 * unique versions for each frameMpName
 * @export
 */
export type FeatureConfigDictionary = Record<keyof IFeatureGroup, Record<string, Array<IFeatureGroup>>> & {
  frameMpWithVersion: Array<IFeatureGroup>;
};
