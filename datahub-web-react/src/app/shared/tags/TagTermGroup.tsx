import { Typography, Button } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { PlusOutlined } from '@ant-design/icons';
import Highlight from 'react-highlighter';

import { useEntityRegistry } from '../../useEntityRegistry';
import { Domain, EntityType, GlobalTags, GlossaryTerms, SubResourceType } from '../../../types.generated';
import { EMPTY_MESSAGES, ANTD_GRAY } from '../../entity/shared/constants';
import { DomainLink } from './DomainLink';
import EditTagTermsModal from './AddTagsTermsModal';
import StyledTerm from './term/StyledTerm';
import Tag from './tag/Tag';

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
    color: ${ANTD_GRAY[7]};
    margin: 0 7px 0 0;
`;

const highlightMatchStyle = { background: '#ffe58f', padding: '0' };

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
    const entityRegistry = useEntityRegistry();
    const [showAddModal, setShowAddModal] = useState(false);
    const [addModalType, setAddModalType] = useState(EntityType.Tag);
    const tagsEmpty = !editableTags?.tags?.length && !uneditableTags?.tags?.length;
    const termsEmpty = !editableGlossaryTerms?.terms?.length && !uneditableGlossaryTerms?.terms?.length;

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
                    {EMPTY_MESSAGES.tags.title}. {EMPTY_MESSAGES.tags.description}
                </Typography.Paragraph>
            )}
            {showEmptyMessage && canAddTerm && termsEmpty && (
                <Typography.Paragraph type="secondary">
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
                    <PlusOutlined />
                    <span>Add Tags</span>
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
                    <PlusOutlined />
                    <span>Add Terms</span>
                </NoElementButton>
            )}
            {showAddModal && !!entityUrn && !!entityType && (
                <EditTagTermsModal
                    type={addModalType}
                    open
                    onCloseModal={() => {
                        onOpenModal?.();
                        setShowAddModal(false);
                        refetch?.();
                    }}
                    resources={[
                        {
                            resourceUrn: entityUrn,
                            subResource: entitySubresource,
                            subResourceType: entitySubresource ? SubResourceType.DatasetField : null,
                        },
                    ]}
                />
            )}
        </>
    );
}
