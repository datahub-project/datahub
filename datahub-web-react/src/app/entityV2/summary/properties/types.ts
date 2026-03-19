import React from 'react';

import { StructuredPropertyFieldsFragment } from '@graphql/fragments.generated';
import { SummaryElementType } from '@types';

export enum PropertyType {
    Domain = 'domain',
    }

export interface AssetProperty {
    name: string;
    type: SummaryElementType;
    icon?: React.ComponentType<any>;
    key?: string;
    structuredProperty?: StructuredPropertyFieldsFragment;
}

export interface PropertyComponentProps {
    property: AssetProperty;
    position: number;
}
