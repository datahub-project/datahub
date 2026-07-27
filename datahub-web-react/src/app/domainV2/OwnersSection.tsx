import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { ActorsSearchSelect } from '@app/entityV2/shared/EntitySearchSelect/ActorsSearchSelect';

const SectionContainer = styled.div`
    margin-bottom: 24px;
`;

interface Props {
    selectedOwnerUrns: string[];
    setSelectedOwnerUrns: (urns: string[]) => void;
    entityUrn?: string;
    isDisabled?: boolean;
    isLoading?: boolean;
}

/**
 * Component for owner selection and management using standard components
 * The goal is to replace sharedV2/owners/OwnersSection.tsx with this component.
 */
const OwnersSection = ({ selectedOwnerUrns, setSelectedOwnerUrns, entityUrn, isDisabled, isLoading }: Props) => {
    const { t } = useTranslation('governance.domain');
    return (
        <SectionContainer>
            <ActorsSearchSelect
                label={t('owners.title')}
                selectedActorUrns={selectedOwnerUrns}
                onUpdate={(selectedActors) => setSelectedOwnerUrns(selectedActors.map((actor) => actor.urn))}
                placeholder={t('owners.searchPlaceholder')}
                entityUrn={entityUrn}
                isDisabled={isDisabled}
                isLoading={isLoading}
                width="full"
                dataTestId="add-owners-select"
            />
        </SectionContainer>
    );
};

export default OwnersSection;
