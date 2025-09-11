import { Button, Editor, Text, Tooltip } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import DescriptionViewer from '@app/entityV2/summary/documentation/DescriptionViewer';
import EditDescriptionModal from '@app/entityV2/summary/documentation/EditDescriptionModal';
import { useDescriptionUtils } from '@app/entityV2/summary/documentation/useDescriptionUtils';
import { useDocumentationPermission } from '@app/entityV2/summary/documentation/useDocumentationPermission';
import AddLinkModal from '@app/entityV2/summary/links/AddLinkModal';
import Links from '@app/entityV2/summary/links/Links';
import { useLinkPermission } from '@app/entityV2/summary/links/useLinkPermission';

const StyledEditor = styled(Editor)<{ $isEditing?: boolean }>`
    border: none;
    margin-top: 4px;
    &&& {
        .remirror-editor {
            padding: 0;
        }
        p:last-of-type {
            margin-bottom: 0;
        }
    }
`;

const SectionHeaderWrapper = styled.div`
    display: flex;
    justify-content: space-between;
`;

const ButtonsWrapper = styled.div`
    display: flex;
    gap: 8px;
`;

const DescriptionContainer = styled.div`
    max-width: 100%;
`;

interface Props {
    hideLinksButton: boolean;
}

export default function AboutSection({ hideLinksButton }: Props) {
    const [showAddLinkModal, setShowAddLinkModal] = useState(false);
    const [showAddDescriptionModal, setShowDescriptionModal] = useState(false);

    const hasLinkPermissions = useLinkPermission();
    const canEditDescription = useDocumentationPermission();
    const {
        displayedDescription,
        updatedDescription,
        setUpdatedDescription,
        handleDescriptionUpdate,
        emptyDescriptionText,
    } = useDescriptionUtils();

    return (
        <div>
            <SectionHeaderWrapper>
                <Text weight="bold" color="gray" colorLevel={600} size="sm">
                    About
                </Text>
                <ButtonsWrapper>
                    {hasLinkPermissions && (
                        <Tooltip title="Add link">
                            <Button
                                variant="text"
                                color="gray"
                                size="xs"
                                icon={{ icon: 'LinkSimple', source: 'phosphor', size: 'lg' }}
                                style={{ padding: '0 2px' }}
                                onClick={() => setShowAddLinkModal(true)}
                            />
                        </Tooltip>
                    )}
                    {canEditDescription && (
                        <Tooltip title="Edit description">
                            <Button
                                variant="text"
                                color="gray"
                                size="xs"
                                icon={{ icon: 'PencilSimpleLine', source: 'phosphor', size: 'lg' }}
                                style={{ padding: '0 2px' }}
                                onClick={() => setShowDescriptionModal(true)}
                            />
                        </Tooltip>
                    )}
                </ButtonsWrapper>
            </SectionHeaderWrapper>
            <DescriptionContainer>
                <DescriptionViewer>
                    <StyledEditor content={displayedDescription} placeholder={emptyDescriptionText} readOnly />
                </DescriptionViewer>
            </DescriptionContainer>
            {!hideLinksButton && <Links />}
            {showAddLinkModal && <AddLinkModal setShowAddLinkModal={setShowAddLinkModal} />}
            {showAddDescriptionModal && (
                <EditDescriptionModal
                    updatedDescription={updatedDescription}
                    setUpdatedDescription={setUpdatedDescription}
                    handleDescriptionUpdate={handleDescriptionUpdate}
                    emptyDescriptionText={emptyDescriptionText}
                    closeModal={() => {
                        setShowDescriptionModal(false);
                        setUpdatedDescription(displayedDescription);
                    }}
                />
            )}
        </div>
    );
}
