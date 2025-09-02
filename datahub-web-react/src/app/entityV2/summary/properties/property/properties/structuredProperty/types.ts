import { PropertyComponentProps } from '@app/entityV2/summary/properties/types';

import { StructuredPropertiesEntry } from '@types';

export interface StructuredPropertyComponentProps extends PropertyComponentProps {
    structuredPropertyEntry?: StructuredPropertiesEntry;
}
