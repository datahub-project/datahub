import { File } from '@phosphor-icons/react/dist/csr/File';
import { PencilSimple } from '@phosphor-icons/react/dist/csr/PencilSimple';
import React, { useCallback, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData, useRouteToTab } from '@app/entity/shared/EntityContext';
import { EditLinkModal } from '@app/entityV2/shared/components/links/EditLinkModal';
import { useLinkListActions } from '@app/entityV2/shared/components/links/useLinkListActions';
import { AddLinkModal } from '@app/entityV2/shared/components/styled/AddLinkModal';
import { EmptyTab } from '@app/entityV2/shared/components/styled/EmptyTab';
import { SectionContainer, SummaryTabHeaderTitle } from '@app/entityV2/shared/summary/HeaderComponents';
import { ResourceLinkPill } from '@app/entityV2/shared/tabs/Documentation/components/ResourceLinkPill';
import { Button, Editor } from '@src/alchemy-components';

const UNEXPANDED_HEIGHT = 2000;
const DOCUMENTATION_TAB_NAME = 'Documentation';

const DocumentationWrapper = styled.div<{ canExpand?: boolean }>`
    position: relative;

    && {
        margin: 0;
        ${({ canExpand }) => canExpand && 'margin-bottom: 50px;'}
    }

    && .ant-empty {
        padding: 0;
    }

    .remirror-editor.ProseMirror {
        padding: 0px 8px;
    }
`;

const EditorWrapper = styled.div<{ mask?: boolean; maxHeight: string }>`
    max-height: ${({ maxHeight }) => maxHeight};
    overflow-y: hidden;
    ${({ mask, theme }) =>
        mask &&
        `-webkit-mask-image: linear-gradient(to bottom, black 50%, ${theme.colors.overlayMask} 60%, transparent 90%);
         mask-image: linear-gradient(to bottom, black 80%, ${theme.colors.overlayMask} 95%, transparent 100%);`}
`;

const ExpandButton = styled(Button)`
    position: absolute;
    left: 50%;
    top: ${UNEXPANDED_HEIGHT + 10}px;
    transform: translateX(-50%);
    z-index: 1;
`;

const LinkPillsContainer = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    margin-top: 8px;
`;

export default function SummaryAboutSection() {
    const { t } = useTranslation('entity.shared.profile');
    const { t: tc } = useTranslation('common.actions');
    const { entityData } = useEntityData();
    const routeToTab = useRouteToTab();
    const links = entityData?.institutionalMemory?.elements || [];
    const { editingMetadata, isEditModalOpen, onEdit, onCloseEditModal, handleDeleteLink } = useLinkListActions();

    const [height, setHeight] = useState(0);
    const measuredRef = useCallback((node) => {
        if (node !== null) {
            const resizeObserver = new ResizeObserver(() => {
                setHeight(node.getBoundingClientRect().height);
            });
            resizeObserver.observe(node);
        }
    }, []);
    const [expanded, setExpanded] = useState(false);
    const maxHeight = expanded ? 'unset' : `${UNEXPANDED_HEIGHT}px`;
    const description = entityData?.editableProperties?.description || entityData?.properties?.description || '';
    const canExpand = !!description && !expanded && height >= UNEXPANDED_HEIGHT;

    return (
        <SectionContainer>
            <SummaryTabHeaderTitle title={t('summary.documentationTitle')} icon={<File />} />
            <DocumentationWrapper canExpand={canExpand ? true : undefined}>
                {!!description && (
                    <>
                        <EditorWrapper ref={measuredRef} mask={canExpand ? true : undefined} maxHeight={maxHeight}>
                            <Editor content={description} readOnly />
                        </EditorWrapper>
                        {canExpand && <ExpandButton onClick={() => setExpanded(true)}>{tc('expand')}</ExpandButton>}
                    </>
                )}
                {!description && (
                    <EmptyTab tab="documentation" hideImage>
                        <AddLinkModal />
                        <Button
                            data-testid="add-documentation"
                            onClick={() =>
                                routeToTab({ tabName: DOCUMENTATION_TAB_NAME, tabParams: { editing: true } })
                            }
                        >
                            <PencilSimple /> {t('summary.addDocumentation')}
                        </Button>
                    </EmptyTab>
                )}
                {links.length > 0 && (
                    <LinkPillsContainer data-testid="link-list">
                        {links.map((link) => (
                            <ResourceLinkPill
                                key={`link-${link.url}`}
                                link={link}
                                onEdit={onEdit}
                                onDelete={handleDeleteLink}
                            />
                        ))}
                    </LinkPillsContainer>
                )}
            </DocumentationWrapper>
            {isEditModalOpen && <EditLinkModal link={editingMetadata} onClose={onCloseEditModal} />}
        </SectionContainer>
    );
}
