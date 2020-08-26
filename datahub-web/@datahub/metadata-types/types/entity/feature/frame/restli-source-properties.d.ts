// Represents a Restli source and related properties
export interface IRestliSourceProperties {
  // Restli resource name
  restResourceName: string;
  // MVEL key expression
  keyExpression: string;
  // Path spec, a comma separated string
  pathSpec: string;
  // A map for specifying additional parameters needed by Rest.li
  restRequestParams: Record<string, string | Array<string> | Record<string, string>>;
}
