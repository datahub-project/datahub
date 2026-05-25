import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import { ActorsSearchSelect } from '@app/entityV2/shared/EntitySearchSelect/ActorsSearchSelect';

const SectionContainer = styled.div`
    margin-bottom: 24px;
`;

const SectionHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 8px;
`;

const FormSection = styled.div`
    margin-bottom: 16px;
`;

// Owners section props
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
    return (
        <SectionContainer>
            <SectionHeader>
                <Text>Add Owners</Text>
            </SectionHeader>
            <FormSection>
                <ActorsSearchSelect
                    selectedActorUrns={selectedOwnerUrns}
                    onUpdate={(selectedActors) => setSelectedOwnerUrns(selectedActors.map((actor) => actor.urn))}
                    placeholder="Search for users or groups"
                    entityUrn={entityUrn}
                    isDisabled={isDisabled}
                    isLoading={isLoading}
                    width="full"
                    dataTestId="add-owners-select"
                />
            </FormSection>
        </SectionContainer>
    );
};

export default OwnersSection;
