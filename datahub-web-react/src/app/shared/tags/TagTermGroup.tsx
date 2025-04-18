import { Tag as AntTag, Typography, Button, message } from 'antd';
import { Tooltip } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';
import { ClockCircleOutlined, PlusOutlined } from '@ant-design/icons';
import { BookmarkSimple } from '@phosphor-icons/react';
import Highlight from 'react-highlighter';

import { useEntityRegistry } from '../../useEntityRegistry';
import {
    Domain,
    ActionRequest,
    EntityType,
    GlobalTags,
    GlossaryTerms,
    SubResourceType,
} from '../../../types.generated';
import { StyledTag } from '../../entity/shared/components/styled/StyledTag';
import analytics, { EntityActionType, EventType } from '../../analytics';
import { EMPTY_MESSAGES, ANTD_GRAY } from '../../entity/shared/constants';
import { DomainLink } from './DomainLink';
import { useAcceptProposalsMutation, useRejectProposalsMutation } from '../../../graphql/actionRequest.generated';
import ProposalModal from './ProposalModal';
import EditTagTermsModal from './AddTagsTermsModal';
import StyledTerm from './term/StyledTerm';
import Tag from './tag/Tag';
import { shouldShowProposeButton } from './utils/proposalUtils';

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

    const [acceptProposalsMutation] = useAcceptProposalsMutation();
    const [rejectProposalsMutation] = useRejectProposalsMutation();
    const [showProposalDecisionModal, setShowProposalDecisionModal] = useState(false);

    const onCloseProposalDecisionModal = (e) => {
        e.stopPropagation();
        setShowProposalDecisionModal(false);
        setTimeout(() => refetch?.(), 2000);
    };

    const onProposalAcceptance = (actionRequest: ActionRequest) => {
        acceptProposalsMutation({ variables: { urns: [actionRequest.urn] } })
            .then(() => {
                if (entityUrn) {
                    analytics.event({
                        type: EventType.EntityActionEvent,
                        actionType: EntityActionType.ProposalAccepted,
                        actionQualifier: actionRequest.type,
                        entityType,
                        entityUrn,
                    });
                }
                message.success('Successfully accepted the proposal!');
            })
            .then(refetch)
            .catch((err) => {
                console.log(err);
                message.error('Failed to accept proposal. :(');
            });
    };

    const onProposalRejection = (actionRequest: ActionRequest) => {
        rejectProposalsMutation({ variables: { urns: [actionRequest.urn] } })
            .then(() => {
                if (entityUrn) {
                    analytics.event({
                        type: EventType.EntityActionEvent,
                        actionType: EntityActionType.ProposalRejected,
                        actionQualifier: actionRequest.type,
                        entityType,
                        entityUrn,
                    });
                }
                message.info('Proposal declined.');
            })
            .then(refetch)
            .catch((err) => {
                console.log(err);
                message.error('Failed to reject proposal. :(');
            });
    };

    const onActionRequestUpdate = () => {
        refetch?.();
    };

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
                                    setShowProposalDecisionModal(true);
                                }}
                            >
                                <BookmarkSimple style={{ marginRight: '3%' }} />
                                {entityRegistry.getDisplayName(EntityType.GlossaryTerm, proposedTerm)}
                                <ProposalModal
                                    actionRequest={actionRequest}
                                    showProposalDecisionModal={showProposalDecisionModal}
                                    onCloseProposalDecisionModal={onCloseProposalDecisionModal}
                                    onProposalAcceptance={onProposalAcceptance}
                                    onProposalRejection={onProposalRejection}
                                    onActionRequestUpdate={onActionRequestUpdate}
                                    elementName={entityRegistry.getDisplayName(EntityType.GlossaryTerm, proposedTerm)}
                                />
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
                                    setShowProposalDecisionModal(true);
                                }}
                            >
                                <span>
                                    {entityRegistry.getDisplayName(EntityType.Tag, proposedTag)}
                                    <ProposalModal
                                        actionRequest={actionRequest}
                                        showProposalDecisionModal={showProposalDecisionModal}
                                        onCloseProposalDecisionModal={onCloseProposalDecisionModal}
                                        onProposalAcceptance={onProposalAcceptance}
                                        onProposalRejection={onProposalRejection}
                                        onActionRequestUpdate={onActionRequestUpdate}
                                        elementName={proposedTag?.properties?.name}
                                    />
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
        </>
    );
}
