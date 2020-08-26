/**
 * Represents an espresso source and related properties.
 * @export
 * @interface IEspressoSourceProperties
 */
export interface IEspressoSourceProperties {
  // Espresso database name
  database: string;
  // Espresso table name
  table: string;
  // D2 URI of the Espresso database
  d2Uri: string;
}
