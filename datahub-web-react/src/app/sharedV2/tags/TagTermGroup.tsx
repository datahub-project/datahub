import { PlusOutlined } from '@ant-design/icons';
import { Typography } from 'antd';
import React, { useState } from 'react';
import Highlight from 'react-highlighter';
import styled from 'styled-components';
import { Domain as DomainEntity, EntityType, GlobalTags, GlossaryTerms } from '../../../types.generated';
import { EMPTY_MESSAGES } from '../../entity/shared/constants';
import { REDESIGN_COLORS } from '../../entityV2/shared/constants';
import { useEntityRegistry } from '../../useEntityRegistry';
import { DomainLink } from './DomainLink';
import Tag from './tag/Tag';
import Term from './term/Term';
import AddTagTerm from './AddTagTerm';

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

    const tagsEmpty = !editableTags?.tags?.length && !uneditableTags?.tags?.length;

    const termsEmpty = !editableGlossaryTerms?.terms?.length && !uneditableGlossaryTerms?.terms?.length;

    const tagsLength = (editableTags?.tags?.length ?? 0) + (uneditableTags?.tags?.length ?? 0);
    const termsLength = (editableGlossaryTerms?.terms?.length ?? 0) + (uneditableGlossaryTerms?.terms?.length ?? 0);

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
                    />
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
                    />
                );
            })}
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
