import React, { useCallback, useMemo } from 'react';
import { useTranslation } from 'react-i18next';

import { ActorsSearchSelect } from '@app/entityV2/shared/EntitySearchSelect/ActorsSearchSelect';
import { ActorEntity } from '@app/entityV2/shared/utils/actorUtils';
import { FieldWrapper } from '@app/ingestV2/source/multiStepBuilder/components/FieldWrapper';

interface Props {
    label: string;
    ownerUrns?: string[];
    updateOwners?: (owners: ActorEntity[]) => void;
    isDisabled?: boolean;
    isLoading?: boolean;
}

export function ActorsField({ label, ownerUrns, updateOwners, isDisabled, isLoading }: Props) {
    const { t } = useTranslation('ingestion.sourceBuilder');
    const urns = useMemo(() => ownerUrns ?? [], [ownerUrns]);
    const onUpdate = useCallback((owners: ActorEntity[]) => updateOwners?.(owners), [updateOwners]);

    return (
        <FieldWrapper label={label}>
            <ActorsSearchSelect
                selectedActorUrns={urns}
                onUpdate={onUpdate}
                placeholder={t('multiStep.builder.actorsPlaceholder')}
                isDisabled={isDisabled}
                isLoading={isLoading}
            />
        </FieldWrapper>
    );
}
