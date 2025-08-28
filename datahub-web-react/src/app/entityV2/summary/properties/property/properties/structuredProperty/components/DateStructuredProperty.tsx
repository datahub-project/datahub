import React from 'react';

import StringStructuredProperty from '@app/entityV2/summary/properties/property/properties/structuredProperty/components/StringStructuredProperty';
import { StructuredPropertyComponentProps } from '@app/entityV2/summary/properties/property/properties/structuredProperty/types';

export default function DateStructuredProperty(props: StructuredPropertyComponentProps) {
    return <StringStructuredProperty {...props} />;
}
