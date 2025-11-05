import { StructuredPropertyFieldsFragment } from '@graphql/fragments.generated';
import { SummaryElementType } from '@types';

export enum PropertyType {
    Domain = 'domain',
    Owners = 'owners',
    Tags = 'tags',
    StructuredProperty = 'structuredProperty',
}

export interface AssetProperty {
    name: string;
    type: SummaryElementType;
    icon?: string;
    key?: string;
    structuredProperty?: StructuredPropertyFieldsFragment;
}

export interface PropertyComponentProps {
    property: AssetProperty;
    position: number;
}
