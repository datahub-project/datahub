import React from 'react';

import BaseProperty from '@app/entityV2/summary/properties/property/properties/BaseProperty';
import { StructuredPropertyComponentProps } from '@app/entityV2/summary/properties/property/properties/structuredProperty/types';

export default function UnknownStructuredProperty(props: StructuredPropertyComponentProps) {
    return <BaseProperty {...props} values={[]} renderValue={(value) => value} />;
}
