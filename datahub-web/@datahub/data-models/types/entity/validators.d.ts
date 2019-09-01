/**
 * Defines the interface for an IDL that specifies the data types for properties on
 * a compliance metadata object
 * @interface IMetadataType
 */
export interface IMetadataType {
  // the expected type or types for the property with @name
  '@type': string | Array<string>;
  // the name of the property that should be on the metadata object
  '@name': string;
  // optional list of properties that are expected on the metadata object
  '@props'?: Array<IMetadataType>;
  // optional list of expected string values for an enum type (not implemented)
  '@symbols'?: Array<string>;
}
