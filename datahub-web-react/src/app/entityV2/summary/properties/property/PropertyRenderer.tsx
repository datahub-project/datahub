import React, { useMemo } from 'react';

import CreatedProperty from '@app/entityV2/summary/properties/property/properties/CreatedProperty';
import DomainProperty from '@app/entityV2/summary/properties/property/properties/DomainProperty';
import LastUpdatedProperty from '@app/entityV2/summary/properties/property/properties/LastUpdatedProperty';
import OwnersProperty from '@app/entityV2/summary/properties/property/properties/OwnersProperty';
import TagsProperty from '@app/entityV2/summary/properties/property/properties/TagsProperty';
import TermsProperty from '@app/entityV2/summary/properties/property/properties/TermsProperty';
import VerificationStatusProperty from '@app/entityV2/summary/properties/property/properties/VerificationStatusProperty';
import StructuredPropertyProperty from '@app/entityV2/summary/properties/property/properties/structuredProperty/StructuredPropertyProperty';
import { PropertyComponentProps, PropertyType } from '@app/entityV2/summary/properties/types';

export default function PropertyRenderer(props: PropertyComponentProps) {
    const { property } = props;
    const PropertyComponent: React.FC<PropertyComponentProps> = useMemo(() => {
        switch (property.type) {
            case PropertyType.Owners:
                return OwnersProperty;
            case PropertyType.Tags:
                return TagsProperty;
            case PropertyType.Terms:
                return TermsProperty;
            case PropertyType.Domain:
                return DomainProperty;
            case PropertyType.Created:
                return CreatedProperty;
            case PropertyType.LastUpdated:
                return LastUpdatedProperty;
            case PropertyType.VerificationStatus:
                return VerificationStatusProperty;
            case PropertyType.StructuredProperty:
                return StructuredPropertyProperty;
            default:
                return () => null;
        }
    }, [property]);

    return <PropertyComponent {...props} />;
}
