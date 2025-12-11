/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useMemo } from 'react';

import CreatedProperty from '@app/entityV2/summary/properties/property/properties/CreatedProperty';
import DomainProperty from '@app/entityV2/summary/properties/property/properties/DomainProperty';
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
            case SummaryElementType.StructuredProperty:
                return StructuredPropertyProperty;
            default:
                return () => null;
        }
    }, [property]);

    return <PropertyComponent {...props} />;
}
