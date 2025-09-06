import { StructuredPropertyFieldsFragment } from '@graphql/fragments.generated';
import { SummaryElementType } from '@types';

export enum PropertyType {
    Domain = 'domain',
    Owners = 'owners',
    Tags = 'tags',
    Terms = 'terms',
    Created = 'created',
    LastUpdated = 'lastUpdated',
    VerificationStatus = 'verificationStatus',
    StructuredProperty = 'structuredProperty',
}

export interface AssetProperty {
    name: string;
    type: SummaryElementType;
    icon?: string;
    key?: string;
    structuredProperty?: StructuredPropertyFieldsFragment;
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
