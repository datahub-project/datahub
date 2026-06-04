// TODO: Migrate the BusinessAttributes list page (`app/businessAttribute/BusinessAttributes.tsx`,
// gated on `useBusinessAttributesFlag`) over to the V2 `app/sharedV2/tags/TagTermGroup` and delete
// this V1 file. The V1 UI was removed in #16680, but this file was left behind because the
// BusinessAttributes list page is behind a feature flag and was missed by that cleanup sweep.
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import { Button, Typography } from 'antd';
import React, { useMemo, useState } from 'react';
import Highlight from 'react-highlighter';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { EMPTY_MESSAGES } from '@app/entity/shared/constants';
import AddTagsModal from '@app/shared/tags/AddTagsModal';
import AddTermsModal from '@app/shared/tags/AddTermsModal';
import { DomainLink } from '@app/shared/tags/DomainLink';
import Tag from '@app/shared/tags/tag/Tag';
import StyledTerm from '@app/shared/tags/term/StyledTerm';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Domain, EntityType, GlobalTags, GlossaryTerms, SubResourceType } from '@types';

type Props = {
    uneditableTags?: GlobalTags | null;
    editableTags?: GlobalTags | null;
    editableGlossaryTerms?: GlossaryTerms | null;
    uneditableGlossaryTerms?: GlossaryTerms | null;
    domain?: Domain | undefined | null;
    canRemove?: boolean;
    canAddTag?: boolean;
    canAddTerm?: boolean;
    showEmptyMessage?: boolean;
    buttonProps?: Record<string, unknown>;
    onOpenModal?: () => void;
    maxShow?: number;
    entityUrn?: string;
    entityType?: EntityType;
    entitySubresource?: string;
    highlightText?: string;
    fontSize?: number;
    refetch?: () => Promise<any>;
    readOnly?: boolean;
};

const NoElementButton = styled(Button)`
    :not(:last-child) {
        margin-right: 8px;
    }
`;

const TagText = styled.span`
    color: ${(props) => props.theme.colors.text};
    margin: 0 7px 0 0;
`;

export default function TagTermGroup({
    uneditableTags,
    editableTags,
    canRemove,
    canAddTag,
    canAddTerm,
    showEmptyMessage,
    buttonProps,
    onOpenModal,
    maxShow,
    uneditableGlossaryTerms,
    editableGlossaryTerms,
    domain,
    entityUrn,
    entityType,
    entitySubresource,
    highlightText,
    fontSize,
    refetch,
    readOnly,
}: Props) {
    const { t } = useTranslation('shared.tags');
    const theme = useTheme();
    const entityRegistry = useEntityRegistry();
    const [showAddModal, setShowAddModal] = useState(false);
    const [addModalType, setAddModalType] = useState(EntityType.Tag);
    const tagsEmpty = !editableTags?.tags?.length && !uneditableTags?.tags?.length;
    const termsEmpty = !editableGlossaryTerms?.terms?.length && !uneditableGlossaryTerms?.terms?.length;
    const highlightMatchStyle = useMemo(
        () => ({ background: theme.colors.bgHighlight, padding: '0' }),
        [theme.colors.bgHighlight],
    );

    let renderedTags = 0;

    return (
        <>
            {domain && (
                <DomainLink domain={domain} name={entityRegistry.getDisplayName(EntityType.Domain, domain) || ''} />
            )}
            {uneditableGlossaryTerms?.terms?.map((term) => {
                renderedTags += 1;
                if (maxShow && renderedTags === maxShow + 1)
                    return (
                        <TagText>
                            <Highlight matchStyle={highlightMatchStyle} search={highlightText}>
                                {uneditableGlossaryTerms?.terms
                                    ? `+${uneditableGlossaryTerms?.terms?.length - maxShow}`
                                    : null}
                            </Highlight>
                        </TagText>
                    );
                if (maxShow && renderedTags > maxShow) return null;

                return (
                    <StyledTerm
                        term={term}
                        entityUrn={entityUrn}
                        entitySubresource={entitySubresource}
                        canRemove={false}
                        readOnly={readOnly}
                        highlightText={highlightText}
                        onOpenModal={onOpenModal}
                        refetch={refetch}
                        fontSize={fontSize}
                    />
                );
            })}
            {editableGlossaryTerms?.terms?.map((term) => (
                <StyledTerm
                    term={term}
                    entityUrn={entityUrn}
                    entitySubresource={entitySubresource}
                    canRemove={canRemove}
                    readOnly={readOnly}
                    highlightText={highlightText}
                    onOpenModal={onOpenModal}
                    refetch={refetch}
                    fontSize={fontSize}
                />
            ))}
            {/* uneditable tags are provided by ingestion pipelines exclusively */}
            {uneditableTags?.tags?.map((tag) => {
                renderedTags += 1;
                if (maxShow && renderedTags === maxShow + 1)
                    return (
                        <TagText>{uneditableTags?.tags ? `+${uneditableTags?.tags?.length - maxShow}` : null}</TagText>
                    );
                if (maxShow && renderedTags > maxShow) return null;

                return (
                    <Tag
                        tag={tag}
                        entityUrn={entityUrn}
                        entitySubresource={entitySubresource}
                        canRemove={false}
                        readOnly={readOnly}
                        highlightText={highlightText}
                        onOpenModal={onOpenModal}
                        refetch={refetch}
                        fontSize={fontSize}
                    />
                );
            })}
            {/* editable tags may be provided by ingestion pipelines or the UI */}
            {editableTags?.tags?.map((tag) => {
                renderedTags += 1;
                if (maxShow && renderedTags > maxShow) return null;

                return (
                    <Tag
                        tag={tag}
                        entityUrn={entityUrn}
                        entitySubresource={entitySubresource}
                        canRemove={canRemove}
                        readOnly={readOnly}
                        highlightText={highlightText}
                        onOpenModal={onOpenModal}
                        refetch={refetch}
                        fontSize={fontSize}
                    />
                );
            })}
            {showEmptyMessage && canAddTag && tagsEmpty && (
                <Typography.Paragraph type="secondary">
                    {/* eslint-disable-next-line i18next/no-literal-string -- (untranslated-text) EMPTY_MESSAGES content from shared constants; only punctuation separator is literal */}
                    {EMPTY_MESSAGES.tags.title}. {EMPTY_MESSAGES.tags.description}
                </Typography.Paragraph>
            )}
            {showEmptyMessage && canAddTerm && termsEmpty && (
                <Typography.Paragraph type="secondary">
                    {/* eslint-disable-next-line i18next/no-literal-string -- (untranslated-text) EMPTY_MESSAGES content from shared constants; only punctuation separator is literal */}
                    {EMPTY_MESSAGES.terms.title}. {EMPTY_MESSAGES.terms.description}
                </Typography.Paragraph>
            )}
            {canAddTag && !readOnly && (
                <NoElementButton
                    type={showEmptyMessage && tagsEmpty ? 'default' : 'text'}
                    onClick={() => {
                        setAddModalType(EntityType.Tag);
                        setShowAddModal(true);
                    }}
                    {...buttonProps}
                >
                    <Plus size={14} weight="bold" />
                    <span>{t('addTagsButton')}</span>
                </NoElementButton>
            )}
            {canAddTerm && !readOnly && (
                <NoElementButton
                    type={showEmptyMessage && termsEmpty ? 'default' : 'text'}
                    onClick={() => {
                        setAddModalType(EntityType.GlossaryTerm);
                        setShowAddModal(true);
                    }}
                    {...buttonProps}
                >
                    <Plus size={14} weight="bold" />
                    <span>{t('addTermsButton')}</span>
                </NoElementButton>
            )}
            {showAddModal &&
                !!entityUrn &&
                !!entityType &&
                (() => {
                    const onClose = () => {
                        onOpenModal?.();
                        setShowAddModal(false);
                        refetch?.();
                    };
                    const resources = [
                        {
                            resourceUrn: entityUrn,
                            subResource: entitySubresource,
                            subResourceType: entitySubresource ? SubResourceType.DatasetField : null,
                        },
                    ];
                    return addModalType === EntityType.Tag ? (
                        <AddTagsModal open onCloseModal={onClose} resources={resources} />
                    ) : (
                        <AddTermsModal open onCloseModal={onClose} resources={resources} />
                    );
                })()}
        </>
    );
}
