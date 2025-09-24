import React from 'react';

import { useEntityContext } from '@app/entity/shared/EntityContext';
import BaseProperty from '@app/entityV2/summary/properties/property/properties/BaseProperty';
import { PropertyComponentProps } from '@app/entityV2/summary/properties/types';
import Term from '@app/sharedV2/tags/term/Term';

import { GlossaryTermAssociation } from '@types';

export default function TermsProperty(props: PropertyComponentProps) {
    const { entityData, loading } = useEntityContext();
    const glossaryTermAssociations = entityData?.glossaryTerms?.terms ?? [];

    const renderTerm = (glossaryTermAssociation: GlossaryTermAssociation) => {
        return <Term term={glossaryTermAssociation} readOnly />;
    };

    return (
        <BaseProperty
            {...props}
            values={glossaryTermAssociations}
            renderValue={renderTerm}
            loading={loading}
            restItemsPillBorderType="square"
        />
    );
}
