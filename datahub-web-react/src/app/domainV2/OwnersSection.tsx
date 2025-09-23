import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import { ActorsSearchSelect } from '@app/entityV2/shared/EntitySearchSelect/ActorsSearchSelect';

import { CorpGroup, CorpUser, Entity, OwnerEntityType } from '@types';

// Interface for pending owner
export interface PendingOwner {
    ownerUrn: string;
    ownerEntityType: OwnerEntityType;
    ownershipTypeUrn: string;
}

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
    defaultOwners?: (CorpGroup | CorpUser)[];
    selectedOwnerUrns: string[];
    setSelectedOwnerUrns: React.Dispatch<React.SetStateAction<string[]>>;
}

/**
 * Component for owner selection and management using standard components
 * The goal is to replace sharedV2/owners/OwnersSection.tsx with this component.
 */
const OwnersSection = ({ defaultOwners: placeholderOwners, selectedOwnerUrns, setSelectedOwnerUrns }: Props) => {
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
                    defaultActors={placeholderOwners}
                    width="full"
                />
            </FormSection>
        </SectionContainer>
    );
};

export default OwnersSection;
