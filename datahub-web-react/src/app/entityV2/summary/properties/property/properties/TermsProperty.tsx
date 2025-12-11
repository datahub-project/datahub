/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
