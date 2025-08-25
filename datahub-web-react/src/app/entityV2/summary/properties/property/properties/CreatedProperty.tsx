import React from 'react';

import BaseProperty from '@app/entityV2/summary/properties/property/properties/BaseProperty';
import { PropertyComponentProps } from '@app/entityV2/summary/properties/types';

export default function CreatedProperty(props: PropertyComponentProps) {
    // TODO: implement
    return <BaseProperty {...props} values={[]} renderValue={() => null} />;
}
