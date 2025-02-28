import { ClockCircleOutlined, PlusOutlined } from '@ant-design/icons';
import { StyledTag } from '@app/entityV2/shared/components/styled/StyledTag';
import { Tag as AntTag, Typography, message } from 'antd';
import { Tooltip } from '@components';
import React, { useState } from 'react';
import Highlight from 'react-highlighter';
import styled from 'styled-components';
import { useAcceptProposalsMutation, useRejectProposalsMutation } from '../../../graphql/actionRequest.generated';
import { ActionRequest, Domain as DomainEntity, EntityType, GlobalTags, GlossaryTerms } from '../../../types.generated';
import { ANTD_GRAY, EMPTY_MESSAGES } from '../../entity/shared/constants';
import { REDESIGN_COLORS } from '../../entityV2/shared/constants';
import ProposalModal from '../../shared/tags/ProposalModal';
import { useEntityRegistry } from '../../useEntityRegistry';
import { DomainLink } from './DomainLink';
import Tag from './tag/Tag';
import Term from './term/Term';
import AddTagTerm from './AddTagTerm';
import { TermRibbon } from './term/TermContent';
import { generateColorFromPalette } from '../../glossaryV2/colorUtils';

type Props = {
    uneditableTags?: GlobalTags | null;
    editableTags?: GlobalTags | null;
    editableGlossaryTerms?: GlossaryTerms | null;
    uneditableGlossaryTerms?: GlossaryTerms | null;
    domain?: DomainEntity | undefined | null;
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
    showOneAndCount?: boolean;
    showAddButton?: boolean;
};

const NoElementButton = styled.div`
    :not(:last-child) {
        margin-right: 8px;
    }
    margin: 0px;
    padding: 0px;
    flex-basis: 100%;
    color: ${REDESIGN_COLORS.DARK_GREY};
    :hover {
        cursor: pointer;
        color: ${REDESIGN_COLORS.LINK_HOVER_BLUE};
    }
`;
const TagTermWrapper = styled.div<{ $showOneAndCount?: boolean }>`
    display: flex;
    flex-wrap: ${(props) => (!props.$showOneAndCount ? 'wrap' : '')};
    align-items: center;
    row-gap: 4px;
    column-gap: 8px;
    max-width: 100%;
`;

const TagText = styled.span`
    color: ${REDESIGN_COLORS.DARK_GREY};
    font-size: 10px;
    font-weight: 400;
    line-height: 8px;
`;

const ProposedTermContainer = styled.div`
    display: flex;
    max-width: inherit;

    .ant-tag.ant-tag {
        border-radius: 5px;
        border: 1px dashed #ccd1dd;
    }
`;

export const ProposedTerm = styled(AntTag)`
    margin: 0;
    padding: 3px 8px;
    font-size: 12px;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
    position: relative;
    overflow: hidden;
    border: 1px dashed;
    display: flex;
    align-items: center;
`;

export const ProposedTag = styled(StyledTag)`
    border: 1px dashed;
`;

const ProposedTermText = styled.span`
    margin-left: 8px;
    text-overflow: ellipsis;
    overflow: hidden;
`;

const StyledPlusOutlined = styled(PlusOutlined)`
    && {
        font-size: 10px;
        margin-right: 8px;
    }
`;

const EmptyText = styled(Typography.Text)`
    && {
        margin-right: 8px;
    }
`;

const Count = styled(Typography.Text)`
    color: ${REDESIGN_COLORS.DARK_GREY};
    font-size: 12px;
    font-weight: 400;
    line-height: 24px;
    overflow: hidden;
    white-space: nowrap;
`;

const AddText = styled.span`
    color: ${REDESIGN_COLORS.DARK_GREY};
    font-size: 12px;
    font-weight: 500;
    line-height: 16px;
    :hover {
        color: ${REDESIGN_COLORS.LINK_HOVER_BLUE};
    }
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
    showOneAndCount,
    showAddButton = true,
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

    const tagsLength =
        (editableTags?.tags?.length ?? 0) + (uneditableTags?.tags?.length ?? 0) + (proposedTags?.length ?? 0);
    const termsLength =
        (editableGlossaryTerms?.terms?.length ?? 0) +
        (uneditableGlossaryTerms?.terms?.length ?? 0) +
        (proposedGlossaryTerms?.length ?? 0);

    let renderedTags = 0;
    let renderedTerms = 0;

    return (
        <TagTermWrapper $showOneAndCount={showOneAndCount}>
            {domain && (
                <DomainLink domain={domain} name={entityRegistry.getDisplayName(EntityType.Domain, domain) || ''} />
            )}
            {uneditableGlossaryTerms?.terms?.map((term) => {
                renderedTerms += 1;
                if (showOneAndCount && renderedTerms === 2) {
                    return <Count>{`+${termsLength - 1}`}</Count>;
                }
                if (showOneAndCount && renderedTerms > 2) return null;
                if (maxShow && renderedTerms === maxShow + 1)
                    return (
                        <TagText>
                            <Highlight matchStyle={highlightMatchStyle} search={highlightText}>
                                {uneditableGlossaryTerms?.terms
                                    ? `+${uneditableGlossaryTerms?.terms?.length - maxShow}`
                                    : null}
                            </Highlight>
                        </TagText>
                    );
                if (maxShow && renderedTerms > maxShow) return null;

                return (
                    <Term
                        term={term}
                        entityUrn={entityUrn}
                        entitySubresource={entitySubresource}
                        canRemove={false}
                        readOnly={readOnly}
                        highlightText={highlightText}
                        onOpenModal={onOpenModal}
                        refetch={refetch}
                        fontSize={fontSize}
                        showOneAndCount={showOneAndCount}
                    />
                );
            })}
            {editableGlossaryTerms?.terms?.map((term) => {
                renderedTerms += 1;
                if (showOneAndCount && renderedTerms === 2) {
                    return <Count>{`+${termsLength - 1}`}</Count>;
                }
                if (showOneAndCount && renderedTerms > 2) return null;
                return (
                    <Term
                        term={term}
                        entityUrn={entityUrn}
                        entitySubresource={entitySubresource}
                        canRemove={canRemove}
                        readOnly={readOnly}
                        highlightText={highlightText}
                        onOpenModal={onOpenModal}
                        refetch={refetch}
                        fontSize={fontSize}
                        showOneAndCount={showOneAndCount}
                        context={term.context}
                    />
                );
            })}
            {proposedGlossaryTerms?.map((actionRequest) => {
                const urn = actionRequest.params?.glossaryTermProposal?.glossaryTerm?.urn;
                const parentNodes = actionRequest.params?.glossaryTermProposal?.glossaryTerm?.parentNodes;
                const lastParentNode = parentNodes && parentNodes.count > 0 && parentNodes.nodes[parentNodes.count - 1];
                const proposedTermColor = lastParentNode
                    ? lastParentNode.displayProperties?.colorHex || generateColorFromPalette(lastParentNode.urn)
                    : (urn && generateColorFromPalette(urn)) || ANTD_GRAY[6];
                return (
                    <ProposedTermContainer>
                        <ProposedTerm
                            closable={false}
                            data-testid={`proposed-term-${actionRequest.params?.glossaryTermProposal?.glossaryTerm?.name}`}
                            onClick={(e) => {
                                e.stopPropagation();
                                setShowProposalDecisionModal(true);
                            }}
                        >
                            <TermRibbon opacity={0.5} color={proposedTermColor} />
                            <ProposedTermText>
                                {entityRegistry.getDisplayName(
                                    EntityType.GlossaryTerm,
                                    actionRequest.params?.glossaryTermProposal?.glossaryTerm,
                                )}
                            </ProposedTermText>
                            <ProposalModal
                                actionRequest={actionRequest}
                                showProposalDecisionModal={showProposalDecisionModal}
                                onCloseProposalDecisionModal={onCloseProposalDecisionModal}
                                onProposalAcceptance={onProposalAcceptance}
                                onProposalRejection={onProposalRejection}
                                onActionRequestUpdate={onActionRequestUpdate}
                                elementName={entityRegistry.getDisplayName(
                                    EntityType.GlossaryTerm,
                                    actionRequest.params?.glossaryTermProposal?.glossaryTerm,
                                )}
                            />
                            <Tooltip overlay="Proposed Term - Pending Approval" showArrow={false}>
                                <ClockCircleOutlined style={{ color: ANTD_GRAY[7], marginLeft: '5px' }} />
                            </Tooltip>
                        </ProposedTerm>
                    </ProposedTermContainer>
                );
            })}

            {/* uneditable tags are provided by ingestion pipelines exclusively  */}

            {uneditableTags?.tags?.map((tag) => {
                renderedTags += 1;
                if (showOneAndCount && renderedTags === 2) {
                    return <Count>{`+${tagsLength - 1}`}</Count>;
                }
                if (showOneAndCount && renderedTags > 2) return null;
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
                        showOneAndCount={showOneAndCount}
                    />
                );
            })}
            {/* editable tags may be provided by ingestion pipelines or the UI */}
            {editableTags?.tags?.map((tag) => {
                renderedTags += 1;
                if (showOneAndCount && renderedTags === 2) {
                    return <Count>{`+${tagsLength - 1}`}</Count>;
                }
                if (showOneAndCount && renderedTags > 2) return null;
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
                        showOneAndCount={showOneAndCount}
                        context={tag.context}
                    />
                );
            })}
            {proposedTags?.map((actionRequest) => (
                <ProposedTag
                    data-testid={`proposed-tag-${actionRequest?.params?.tagProposal?.tag?.properties?.name}`}
                    $colorHash={actionRequest?.params?.tagProposal?.tag?.urn}
                    $color={actionRequest?.params?.tagProposal?.tag?.properties?.colorHex}
                    $showOneAndCount={showOneAndCount}
                    onClick={(e) => {
                        e.stopPropagation();
                        setShowProposalDecisionModal(true);
                    }}
                >
                    <span>
                        {entityRegistry.getDisplayName(EntityType.Tag, actionRequest?.params?.tagProposal?.tag)}
                        <ProposalModal
                            actionRequest={actionRequest}
                            showProposalDecisionModal={showProposalDecisionModal}
                            onCloseProposalDecisionModal={onCloseProposalDecisionModal}
                            onProposalAcceptance={onProposalAcceptance}
                            onProposalRejection={onProposalRejection}
                            onActionRequestUpdate={onActionRequestUpdate}
                            elementName={actionRequest?.params?.tagProposal?.tag?.properties?.name}
                        />
                        <Tooltip overlay="Proposed Tag - Pending Approval" showArrow={false}>
                            <ClockCircleOutlined style={{ color: ANTD_GRAY[7], marginLeft: '5px' }} />
                        </Tooltip>
                    </span>
                </ProposedTag>
            ))}
            {showEmptyMessage && canAddTag && tagsEmpty && (
                <EmptyText type="secondary">{EMPTY_MESSAGES.tags.title}.</EmptyText>
            )}
            {showEmptyMessage && canAddTerm && termsEmpty && (
                <EmptyText type="secondary">{EMPTY_MESSAGES.terms.title}.</EmptyText>
            )}
            {canAddTag && !readOnly && showAddButton && (
                <NoElementButton
                    onClick={() => {
                        setAddModalType(EntityType.Tag);
                        setShowAddModal(true);
                    }}
                    {...buttonProps}
                >
                    <StyledPlusOutlined />
                    <AddText>Add tags</AddText>
                </NoElementButton>
            )}
            {canAddTerm && !readOnly && showAddButton && (
                <NoElementButton
                    onClick={() => {
                        setAddModalType(EntityType.GlossaryTerm);
                        setShowAddModal(true);
                    }}
                    {...buttonProps}
                >
                    <StyledPlusOutlined />
                    <AddText>Add terms</AddText>
                </NoElementButton>
            )}
            <AddTagTerm
                onOpenModal={onOpenModal}
                entityUrn={entityUrn}
                entityType={entityType}
                entitySubresource={entitySubresource}
                showAddModal={showAddModal}
                setShowAddModal={setShowAddModal}
                addModalType={addModalType}
                refetch={refetch}
            />
        </TagTermWrapper>
    );
}
