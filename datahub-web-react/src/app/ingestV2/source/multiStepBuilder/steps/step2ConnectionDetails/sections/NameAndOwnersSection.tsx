import { spacing } from '@components';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { ActorEntity } from '@app/entityV2/shared/utils/actorUtils';
import { ActorsField } from '@app/ingestV2/source/multiStepBuilder/components/ActorsField';
import { Field } from '@app/ingestV2/source/multiStepBuilder/components/Field';
import { MAX_FORM_WIDTH } from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/constants';

import { IngestionSource } from '@types';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: ${spacing.sm};
    max-width: ${MAX_FORM_WIDTH};
`;

interface Props {
    source?: IngestionSource;
    sourceName?: string;
    updateSourceName?: (newSourceName: string) => void;
    ownerUrns?: string[];
    updateOwners?: (owners: ActorEntity[]) => void;
    isEditing?: boolean;
}

export function NameAndOwnersSection({
    source,
    sourceName,
    updateSourceName,
    ownerUrns,
    updateOwners,
    isEditing,
}: Props) {
    const me = useUserContext();

    const existingOwners = useMemo(() => source?.ownership?.owners || [], [source]);
    const defaultActors = useMemo(() => {
        if (!isEditing && me.user) {
            return [me.user];
        }
        return existingOwners.map((owner) => owner.owner);
    }, [existingOwners, isEditing, me.user]);

    return (
        <Container>
            <Field
                label="Source Name"
                name="source_name"
                value={sourceName}
                onChange={updateSourceName}
                placeholder="Give data source a name"
                required
            />

            <ActorsField
                label="Add Owners"
                ownerUrns={ownerUrns}
                updateOwners={updateOwners}
                defaultActors={defaultActors}
            />
        </Container>
    );
}
