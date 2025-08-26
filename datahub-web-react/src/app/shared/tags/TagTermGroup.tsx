import { ClockCircleOutlined, PlusOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { BookmarkSimple } from '@phosphor-icons/react';
import { Tag as AntTag, Button, Typography } from 'antd';
import React, { useState } from 'react';
import Highlight from 'react-highlighter';
import styled from 'styled-components';

import { StyledTag } from '@app/entity/shared/components/styled/StyledTag';
import { ANTD_GRAY, EMPTY_MESSAGES } from '@app/entity/shared/constants';
import EditTagTermsModal from '@app/shared/tags/AddTagsTermsModal';
import { DomainLink } from '@app/shared/tags/DomainLink';
import ProposalModal from '@app/shared/tags/ProposalModal';
import Tag from '@app/shared/tags/tag/Tag';
import StyledTerm from '@app/shared/tags/term/StyledTerm';
import { shouldShowProposeButton } from '@app/shared/tags/utils/proposalUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { ActionRequest, Domain, EntityType, GlobalTags, GlossaryTerms, SubResourceType } from '@types';

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

    proposedGlossaryTerms?: ActionRequest[];
    proposedTags?: ActionRequest[];
};

const NoElementButton = styled(Button)`
    :not(:last-child) {
        margin-right: 8px;
    }
`;

const TagText = styled.span`
    color: ${ANTD_GRAY[7]};
`;

const ProposedTerm = styled(AntTag)`
    opacity: 0.7;
    border-style: dashed;
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
    proposedGlossaryTerms,
    proposedTags,
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

    const [selectedActionRequest, setSelectedActionRequest] = useState<ActionRequest | null | undefined>(null);

    const tagsEmpty = !editableTags?.tags?.length && !uneditableTags?.tags?.length && !proposedTags?.length;
    const termsEmpty =
        !editableGlossaryTerms?.terms?.length &&
        !uneditableGlossaryTerms?.terms?.length &&
        !proposedGlossaryTerms?.length;

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
            {proposedGlossaryTerms?.map((actionRequest) => {
                const proposedTerm =
                    actionRequest.params?.glossaryTermProposal?.glossaryTerm ||
                    actionRequest.params?.glossaryTermProposal?.glossaryTerms?.[0];

                return (
                    <>
                        {proposedTerm && (
                            <ProposedTerm
                                closable={false}
                                data-testid={`proposed-term-${proposedTerm?.name}`}
                                onClick={() => {
                                    setSelectedActionRequest(actionRequest);
                                }}
                            >
                                <BookmarkSimple style={{ marginRight: '3%' }} />
                                {entityRegistry.getDisplayName(EntityType.GlossaryTerm, proposedTerm)}
                                <Tooltip overlay="Proposed Term - Pending Approval" showArrow={false}>
                                    <ClockCircleOutlined style={{ color: ANTD_GRAY[7], marginLeft: '5px' }} />
                                </Tooltip>
                            </ProposedTerm>
                        )}
                    </>
                );
            })}
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
            {proposedTags?.map((actionRequest) => {
                const proposedTag =
                    actionRequest?.params?.tagProposal?.tag || actionRequest?.params?.tagProposal?.tags?.[0];
                return (
                    <>
                        {proposedTag && (
                            <StyledTag
                                data-testid={`proposed-tag-${proposedTag?.properties?.name}`}
                                $colorHash={proposedTag?.urn}
                                $color={proposedTag?.properties?.colorHex}
                                onClick={() => {
                                    setSelectedActionRequest(actionRequest);
                                }}
                            >
                                <span>
                                    {entityRegistry.getDisplayName(EntityType.Tag, proposedTag)}
                                    <Tooltip overlay="Proposed Tag - Pending Approval" showArrow={false}>
                                        <ClockCircleOutlined style={{ color: ANTD_GRAY[7], marginLeft: '5px' }} />
                                    </Tooltip>
                                </span>
                            </StyledTag>
                        )}
                    </>
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
                        setTimeout(() => refetch?.(), 2000);
                    }}
                    resources={[
                        {
                            resourceUrn: entityUrn,
                            subResource: entitySubresource,
                            subResourceType: entitySubresource ? SubResourceType.DatasetField : null,
                        },
                    ]}
                    showPropose={shouldShowProposeButton(entityType)}
                    entityType={entityType}
                />
            )}
            {selectedActionRequest && (
                <ProposalModal
                    actionRequest={selectedActionRequest}
                    selectedActionRequest={selectedActionRequest}
                    setSelectedActionRequest={setSelectedActionRequest}
                    refetch={refetch}
                />
            )}
        </>
    );
}
