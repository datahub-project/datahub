import { Button, Editor, Text, Tooltip } from '@components';
import queryString from 'query-string';
import React, { useEffect, useState } from 'react';
import { useHistory, useLocation } from 'react-router-dom';
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
    const history = useHistory();
    const { search, pathname } = useLocation();
    const isEditingDescription = !!queryString.parse(search, { parseBooleans: true }).editingDescription;

    const [showAddLinkModal, setShowAddLinkModal] = useState(false);
    const [showAddDescriptionModal, setShowDescriptionModal] = useState(isEditingDescription);

    const hasLinkPermissions = useLinkPermission();
    const canEditDescription = useDocumentationPermission();
    const {
        displayedDescription,
        updatedDescription,
        setUpdatedDescription,
        handleDescriptionUpdate,
        emptyDescriptionText,
    } = useDescriptionUtils();

    useEffect(() => {
        setShowDescriptionModal(isEditingDescription);
    }, [isEditingDescription]);

    const removeEditingParam = () => {
        const params = queryString.parse(search);
        delete params.editingDescription;

        const newSearch = queryString.stringify(params);
        history.replace({
            pathname,
            search: newSearch,
        });
    };

    const cancelUpdate = () => {
        setShowDescriptionModal(false);
        setUpdatedDescription(displayedDescription);
        removeEditingParam();
    };

    return (
        <div data-testid="about-section">
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
                                data-testid="add-link-button"
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
                                data-testid="edit-description-button"
                            />
                        </Tooltip>
                    )}
                </ButtonsWrapper>
            </SectionHeaderWrapper>
            <DescriptionContainer>
                <DescriptionViewer>
                    <StyledEditor
                        content={displayedDescription}
                        placeholder={emptyDescriptionText}
                        dataTestId="description-viewer"
                        readOnly
                    />
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
                    closeModal={cancelUpdate}
                />
            )}
        </div>
    );
}
