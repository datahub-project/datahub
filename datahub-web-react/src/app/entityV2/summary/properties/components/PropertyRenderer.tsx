import React, { useMemo } from 'react';

import CreatedProperty from '@app/entityV2/summary/properties/components/properties/CreatedProperty';
import DomainProperty from '@app/entityV2/summary/properties/components/properties/DomainProperty';
import LastUpdatedProperty from '@app/entityV2/summary/properties/components/properties/LastUpdatedProperty';
import OwnersProperty from '@app/entityV2/summary/properties/components/properties/OwnersProperty';
import StructuredPropertyProperty from '@app/entityV2/summary/properties/components/properties/StructuredPropertyProperty';
import TagsProperty from '@app/entityV2/summary/properties/components/properties/TagsProperty';
import TermsProperty from '@app/entityV2/summary/properties/components/properties/TermsProperty';
import VerificationStatusProperty from '@app/entityV2/summary/properties/components/properties/VerificationStatusProperty';
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
