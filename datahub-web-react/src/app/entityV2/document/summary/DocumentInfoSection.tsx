import React from 'react';
import styled from 'styled-components';

import { useDocumentPermissions } from '@app/documentV2/hooks/useDocumentPermissions';
import { useUpdateDocument } from '@app/documentV2/hooks/useUpdateDocument';
import { useEntityData } from '@app/entity/shared/EntityContext';
import { SectionContainer } from '@app/entityV2/shared/summary/HeaderComponents';
import { Select } from '@src/alchemy-components';

import { Document, DocumentState } from '@types';

const SectionHeader = styled.h4`
    font-size: 16px;
    font-weight: 600;
    margin: 0;
    color: #262626;
`;

const InfoGrid = styled.div`
    display: grid;
    grid-template-columns: 120px 1fr;
    gap: 12px;
    padding: 16px 0;
`;

const InfoLabel = styled.div`
    color: #595959;
    font-size: 14px;
    font-weight: 500;
`;

const InfoValue = styled.div`
    color: #262626;
    font-size: 14px;
`;

const StatusSelectWrapper = styled.div`
    max-width: 320px;
`;

export const DocumentInfoSection = () => {
    const { urn, entityData } = useEntityData();
    const document = entityData as Document;
    const { canChangeStatus } = useDocumentPermissions(urn);
    const { updateStatus } = useUpdateDocument();

    if (!document?.info) {
        return null;
    }

    const { info } = document;
    const { created, lastModified, parentDocument, draftOf } = info;

    const handleStatusChange = async (value: string) => {
        await updateStatus({
            urn,
            state: value as DocumentState,
        });
    };

    const statusOptions = [
        {
            label: 'Published - Visible to Ask DataHub AI agents',
            value: DocumentState.Published,
        },
        {
            label: 'Unpublished - Hidden from Ask DataHub AI agents',
            value: DocumentState.Unpublished,
        },
    ];

    return (
        <SectionContainer>
            <SectionHeader>Document Information</SectionHeader>
            <InfoGrid>
                {info.status && (
                    <>
                        <InfoLabel>Ask DataHub Status</InfoLabel>
                        <InfoValue>
                            <StatusSelectWrapper>
                                <Select
                                    values={[info.status.state]}
                                    onUpdate={(values) => handleStatusChange(values[0] as string)}
                                    isDisabled={!canChangeStatus}
                                    options={statusOptions}
                                />
                            </StatusSelectWrapper>
                        </InfoValue>
                    </>
                )}

                {created && (
                    <>
                        <InfoLabel>Created By</InfoLabel>
                        <InfoValue>
                            {created.actor ? `${created.actor} · ` : ''}
                            {new Date(created.time).toLocaleDateString('en-US', {
                                year: 'numeric',
                                month: 'long',
                                day: 'numeric',
                            })}
                        </InfoValue>
                    </>
                )}

                {document.subType && (
                    <>
                        <InfoLabel>Type</InfoLabel>
                        <InfoValue>{document.subType}</InfoValue>
                    </>
                )}

                {lastModified && (
                    <>
                        <InfoLabel>Last Modified</InfoLabel>
                        <InfoValue>
                            {new Date(lastModified.time).toLocaleDateString('en-US', {
                                year: 'numeric',
                                month: 'long',
                                day: 'numeric',
                            })}
                        </InfoValue>
                    </>
                )}

                {parentDocument?.document && (
                    <>
                        <InfoLabel>Parent Document</InfoLabel>
                        <InfoValue>{parentDocument.document.info?.title || parentDocument.document.urn}</InfoValue>
                    </>
                )}

                {draftOf?.document && (
                    <>
                        <InfoLabel>Draft Of</InfoLabel>
                        <InfoValue>{draftOf.document.info?.title || draftOf.document.urn}</InfoValue>
                    </>
                )}
            </InfoGrid>
        </SectionContainer>
    );
};
