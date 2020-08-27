/**
 * Properties associated with the multiproduct in which the feature is defined
 * @export
 * @interface IFeatureMultiProductInfo
 */
export interface IFeatureMultiProductInfo {
  // Urn of the frame-feature multiproduct in which this feature has been defined
  multiproduct: string;
  // Name of the offline module of frame-feature multiproduct in which this feature has been defined
  offlineModuleName?: string;
  // Name of the online module of frame-feature multiproduct in which this feature has been defined
  onlineModuleName?: string;
  // The canonical name of the frame feature Multiproduct
  name: string;
  // Version of the multiproduct for onboarding the feature
  version: string;
}
