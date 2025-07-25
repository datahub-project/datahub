import { Avatar } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { AvatarType } from '@components/components/AvatarStack/types';

import { useEntityRegistry } from '@app/useEntityRegistry';
import { StyledFormItem } from '@app/workflows/components/shared';

import { EntityType } from '@types';

const FormSection = styled.div`
    margin-bottom: 24px;
`;

export interface ReviewContextData {
    currentStep?: string;
    requestUrn?: string;
    createdBy?: string;
    createdAt?: number;
    createdActor?: any; // Actor entity with image information
}

export interface ReviewerContextProps {
    reviewContext: ReviewContextData;
}

export const ReviewerContext: React.FC<ReviewerContextProps> = ({ reviewContext }) => {
    const entityRegistry = useEntityRegistry();

    if (!reviewContext.createdBy && !reviewContext.createdAt) {
        return null;
    }

    return (
        <FormSection>
            <StyledFormItem label="Requested by">
                <Link to={`${entityRegistry.getEntityUrl(EntityType.CorpUser, reviewContext.createdActor?.urn || '')}`}>
                    <Avatar
                        name={
                            reviewContext.createdActor
                                ? entityRegistry.getDisplayName(EntityType.CorpUser, reviewContext.createdActor)
                                : reviewContext.createdBy || 'Unknown User'
                        }
                        imageUrl={
                            reviewContext.createdActor?.editableInfo?.pictureLink ||
                            reviewContext.createdActor?.editableProperties?.pictureLink
                        }
                        size="default"
                        type={AvatarType.user}
                        showInPill
                    />
                </Link>
            </StyledFormItem>
        </FormSection>
    );
};
