import React, { useMemo } from 'react';

import CreatedProperty from '@app/entityV2/summary/properties/property/properties/CreatedProperty';
import DocumentStatusProperty from '@app/entityV2/summary/properties/property/properties/DocumentStatusProperty';
import DocumentTypeProperty from '@app/entityV2/summary/properties/property/properties/DocumentTypeProperty';
import DomainProperty from '@app/entityV2/summary/properties/property/properties/DomainProperty';
import LastUpdatedProperty from '@app/entityV2/summary/properties/property/properties/LastUpdatedProperty';
import OwnersProperty from '@app/entityV2/summary/properties/property/properties/OwnersProperty';
import TagsProperty from '@app/entityV2/summary/properties/property/properties/TagsProperty';
import TermsProperty from '@app/entityV2/summary/properties/property/properties/TermsProperty';
import StructuredPropertyProperty from '@app/entityV2/summary/properties/property/properties/structuredProperty/StructuredPropertyProperty';
import { PropertyComponentProps } from '@app/entityV2/summary/properties/types';

import { SummaryElementType } from '@types';

export default function PropertyRenderer(props: PropertyComponentProps) {
    const { property } = props;
    const PropertyComponent: React.FC<PropertyComponentProps> = useMemo(() => {
        switch (property.type) {
            case SummaryElementType.Owners:
                return OwnersProperty;
            case SummaryElementType.Tags:
                return TagsProperty;
            case SummaryElementType.GlossaryTerms:
                return TermsProperty;
            case SummaryElementType.Domain:
                return DomainProperty;
            case SummaryElementType.Created:
                return CreatedProperty;
            case SummaryElementType.LastModified:
                return LastUpdatedProperty;
            case SummaryElementType.StructuredProperty:
                return StructuredPropertyProperty;
            case SummaryElementType.DocumentStatus:
                return DocumentStatusProperty;
            case SummaryElementType.DocumentType:
                return DocumentTypeProperty;
            default:
                return () => null;
        }
    }, [property]);

    return <PropertyComponent {...props} />;
}
