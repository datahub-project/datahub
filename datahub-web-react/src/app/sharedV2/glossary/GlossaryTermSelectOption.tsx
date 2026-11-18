import React from 'react';

import Term from '@app/sharedV2/tags/term/Term';

import { Entity, GlossaryTerm, GlossaryTermAssociation } from '@types';

export function GlossaryTermSelectOption({ entity }: { entity: Entity }) {
    const glossaryTerm = entity as GlossaryTerm;
    const termAssociation: GlossaryTermAssociation = {
        term: glossaryTerm,
        associatedUrn: '',
    };

    return <Term term={termAssociation} readOnly fontSize={14} enableTooltip={false} />;
}
