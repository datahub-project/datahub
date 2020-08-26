interface IFrameDerivedFeatureInput {
  // Name of the input
  name: string;
  // Key associated with the input
  key: string | Array<string>;
  // Feature associated with this input
  feature: string;
}

/**
 * Properties associated with a derived feature as specified in derivations section of frame config
 * @export
 * @interface IFrameDerivedFeatureConfig
 */
export interface IFrameDerivedFeatureConfig {
  // Name of the feature
  name: string;
  // Derived feature expression or class name
  expression: string;
  // Key expression for the feature
  key?: string | Array<string>;
  // Inputs for this derived feature
  inputs?: Array<IFrameDerivedFeatureInput>;
  // Base or upstream features associated with this derived feature
  baseFeatures: Array<string>;
}
