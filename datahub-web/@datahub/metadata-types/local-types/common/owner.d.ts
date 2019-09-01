/**
 * Defines the expected parts of an owner urn, additionally tacks on the full urn string as part of the expected shape
 * @interface IOwnerUrnParts
 */
interface IOwnerUrnParts {
  type: string;
  name: string;
  _inputUrn: string;
}

/**
 * Creates an alias for an object that contains only parts of the urn string
 * @alias Partial<IOwnerUrnParts>
 */
export type OwnerUrnObject = Partial<IOwnerUrnParts>;
