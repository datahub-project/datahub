import React from 'react';
import { EntityType } from '../../../../types.generated';
import EntitySearchResult from '../../../shared/entitySearch/EntitySearchResult';
import { Subview } from '../../user/Subview';

type Props = {
    urn: string;
    ownerships: { [key in EntityType]?: any[] };
    subview?: Subview;
    item?: string;
};

export default function GlossaryTermDetails({ urn, subview, item, ownerships }: Props) {
    return (
        <EntitySearchResult
            urn={urn}
            subview={subview}
            item={item}
            ownerships={ownerships}
            entityType={EntityType.GlossaryTerm}
        />
    );
}
