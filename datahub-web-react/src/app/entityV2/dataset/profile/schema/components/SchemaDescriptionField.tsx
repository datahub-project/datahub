import { FetchResult } from '@apollo/client';
import { Button, Editor, Icon, Text, Tooltip, toast } from '@components';
import { PencilSimple } from '@phosphor-icons/react/dist/csr/PencilSimple';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import analytics, { EntityActionType, EventType } from '@app/analytics';
import { useEntityData } from '@app/entity/shared/EntityContext';
import UpdateDescriptionModal from '@app/entityV2/shared/components/legacy/DescriptionModal';
import { removeMarkdown } from '@app/entityV2/shared/components/styled/StripMarkdownText';
import SchemaEditableContext from '@app/shared/SchemaEditableContext';
import HoverCardAttributionDetails from '@app/sharedV2/propagation/HoverCardAttributionDetails';
import CompactMarkdownViewer from '@src/app/entityV2/shared/tabs/Documentation/components/CompactMarkdownViewer';

import { UpdateDatasetMutation } from '@graphql/dataset.generated';
import { MetadataAttribution } from '@types';

const EditIconButton = styled.button`
    cursor: pointer;
    display: none;
    background: none;
    border: none;
    padding: 0;
    color: ${(props) => props.theme.colors.iconSuccess};
`;

const AddNewDescription = styled(Button)`
    display: flex;
    min-width: 140px;
    background-color: ${(props) => props.theme.colors.bgSurface};
    border-radius: 4px;
    align-items: center;
    justify-content: center;
`;

const ExpandedActions = styled.div`
    height: 10px;
`;

const DescriptionContainer = styled.div`
    position: relative;
    display: inline-block;
    text-overflow: ellipsis;
    overflow: hidden;
    white-space: nowrap;
    width: 100%;
    min-height: 22px;
    font-size: 12px;
    font-weight: 400;
    line-height: 24px;
    color: ${(props) => props.theme.colors.text};
    vertical-align: middle;
    &:hover ${EditIconButton} {
        display: inline-flex;
        align-items: center;
    }

    & ins.diff {
        background-color: ${(props) => props.theme.colors.bgSurfaceSuccess};
        text-decoration: none;
        &:hover {
            background-color: ${(props) => props.theme.colors.bgSurfaceSuccess};
        }
    }
    & del.diff {
        background-color: ${(props) => props.theme.colors.bgSurfaceError};
        text-decoration: line-through;
        &: hover {
            background-color: ${(props) => props.theme.colors.bgSurfaceError};
        }
    }
`;
const EditedLabel = styled(Text)`
    display: inline-block;
    margin-left: 8px;
    color: ${(props) => props.theme.colors.textTertiary};
    font-style: italic;
    position: relative;
    top: -2px;
`;

const ReadLessText = styled(Text)`
    margin-right: 4px;
    cursor: pointer;
    color: ${(props) => props.theme.colors.hyperlinks};
`;

const StyledViewer = styled(Editor)`
    padding-right: 8px;
    display: block;

    .remirror-editor.ProseMirror {
        padding: 0;
        font-size: 12px;
        font-weight: 400;
        line-height: 24px;
        color: ${(props) => props.theme.colors.text};
        vertical-align: middle;
    }
`;

const DescriptionWrapper = styled.span`
    display: inline-flex;
    align-items: center;
    width: 100%;
`;

const AddModalWrapper = styled.div``;

type Props = {
    onExpanded: (expanded: boolean) => void;
    expanded: boolean;
    description: string;
    original?: string | null;
    onUpdate: (
        description: string,
    ) => Promise<FetchResult<UpdateDatasetMutation, Record<string, any>, Record<string, any>> | void>;
    isEdited?: boolean;
    isReadOnly?: boolean;
    isPropagated?: boolean;
    attribution?: MetadataAttribution | null;
    dataTestId?: string;
};

export default function DescriptionField({
    expanded,
    onExpanded: handleExpanded,
    description,
    onUpdate,
    isEdited = false,
    original,
    isReadOnly,
    isPropagated,
    attribution,
    dataTestId,
}: Props) {
    const { t } = useTranslation('entity.types');
    const { t: tc } = useTranslation('common.actions');
    const { t: tf } = useTranslation('common.feedback');
    const [showAddModal, setShowAddModal] = useState(false);

    const overLimit = removeMarkdown(description).length > 40;
    const isSchemaEditable = React.useContext(SchemaEditableContext);
    const onCloseModal = () => {
        setShowAddModal(false);
    };
    const { urn, entityType } = useEntityData();

    const sendAnalytics = () => {
        analytics.event({
            type: EventType.EntityActionEvent,
            actionType: EntityActionType.UpdateSchemaDescription,
            entityType,
            entityUrn: urn,
        });
    };

    const onUpdateModal = async (desc: string | null) => {
        toast.loading(tf('updating'));
        try {
            await onUpdate(desc || '');
            toast.destroy();
            toast.success(tf('updated'), { duration: 2 });
            sendAnalytics();
        } catch (e: unknown) {
            toast.destroy();
            if (e instanceof Error)
                toast.error(t('dataset.updateDescriptionError', { error: e.message || '' }), { duration: 2 });
        }
        onCloseModal();
    };

    const enableEdits = isSchemaEditable && !isReadOnly;
    const EditButton =
        (enableEdits && description && (
            <EditIconButton type="button" onClick={() => setShowAddModal(true)} aria-label={tc('edit')}>
                <Icon icon={PencilSimple} size="md" color="inherit" />
            </EditIconButton>
        )) ||
        undefined;

    const showAddButton = enableEdits && !description;

    return (
        <DescriptionContainer data-testid={dataTestId}>
            {expanded ? (
                <>
                    {!!description && <StyledViewer content={description} readOnly />}
                    {!!description && (EditButton || overLimit) && (
                        <ExpandedActions>
                            {overLimit && (
                                <ReadLessText
                                    type="span"
                                    size="sm"
                                    onClick={(e) => {
                                        e.stopPropagation();
                                        handleExpanded(false);
                                    }}
                                >
                                    {tc('readLess')}
                                </ReadLessText>
                            )}
                            {EditButton}
                        </ExpandedActions>
                    )}
                </>
            ) : (
                description && (
                    <Tooltip
                        title={isPropagated && <HoverCardAttributionDetails propagationDetails={{ attribution }} />}
                    >
                        <DescriptionWrapper>
                            <CompactMarkdownViewer
                                content={description}
                                lineLimit={1}
                                fixedLineHeight
                                customStyle={{ fontSize: '12px' }}
                                scrollableY={false}
                            />
                            {isSchemaEditable && isEdited && (
                                <EditedLabel type="span" size="sm">
                                    {t('dataset.editedLabel')}
                                </EditedLabel>
                            )}
                        </DescriptionWrapper>
                    </Tooltip>
                )
            )}
            {showAddModal && (
                <AddModalWrapper onClick={(e) => e.stopPropagation()}>
                    <UpdateDescriptionModal
                        title={description ? t('dataset.updateDescriptionTitle') : t('dataset.addDescriptionTitle')}
                        description={description}
                        original={original || ''}
                        onClose={onCloseModal}
                        onSubmit={onUpdateModal}
                        isAddDesc={!description}
                    />
                </AddModalWrapper>
            )}
            {showAddButton && (
                <AddNewDescription
                    variant="text"
                    onClick={(e) => {
                        setShowAddModal(true);
                        e.stopPropagation();
                    }}
                >
                    {t('dataset.addDescriptionButton')}
                </AddNewDescription>
            )}
        </DescriptionContainer>
    );
}
