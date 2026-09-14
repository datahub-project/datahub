import React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import MoveDataProductModal from '@app/entityV2/shared/EntityDropdown/MoveDataProductModal';
import MoveDomainModal from '@app/entityV2/shared/EntityDropdown/MoveDomainModal';
import MoveGlossaryEntityModal from '@app/entityV2/shared/EntityDropdown/MoveGlossaryEntityModal';

import { EntityType } from '@types';

type Props = {
    entityType: EntityType;
    entityData: GenericEntityProperties | null;
    urn: string;
    onClose: () => void;
};

export default function MoveEntityModal({ entityType, entityData, urn, onClose }: Props) {
    switch (entityType) {
        case EntityType.GlossaryNode:
        case EntityType.GlossaryTerm:
            return (
                <MoveGlossaryEntityModal entityData={entityData} entityType={entityType} urn={urn} onClose={onClose} />
            );
        case EntityType.Domain:
            return <MoveDomainModal onClose={onClose} />;
        case EntityType.DataProduct:
            return <MoveDataProductModal onClose={onClose} />;
        default:
            return null;
    }
}
