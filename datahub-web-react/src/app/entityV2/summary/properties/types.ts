export enum PropertyType {
    Domain = 'domain',
    Owners = 'owners',
    Tags = 'tags',
    Terms = 'terms',
    Created = 'created',
    LastUpdated = 'last_updated',
    VerificationStatus = 'verification_status',
    StructuredProperty = 'structuredProperty',
}

export interface AssetProperty {
    name: string;
    type: PropertyType;
    icon?: string;
    key?: string;
    structuredPropertyUrn?: string;
}

export interface AssetPropertiesContextType {
    // Whether properties are editable
    editable?: boolean;

    properties: AssetProperty[];
    propertiesLoading?: boolean;

    replace: (newProperty: AssetProperty, position: number) => void;
    remove: (position: number) => void;
    add: (newProperty: AssetProperty) => void;
}

export interface PropertyComponentProps {
    property: AssetProperty;
    position: number;
}
