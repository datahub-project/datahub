import { Modal, Tag, Typography, Button, message, Tooltip } from 'antd';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { BookOutlined, ClockCircleOutlined, PlusOutlined } from '@ant-design/icons';
import { useEntityRegistry } from '../../useEntityRegistry';
import {
    Domain,
    ActionRequest,
    EntityType,
    GlobalTags,
    GlossaryTerms,
    SubResourceType,
} from '../../../types.generated';
import AddTagTermModal from './AddTagTermModal';
import { StyledTag } from '../../entity/shared/components/styled/StyledTag';
import { EMPTY_MESSAGES } from '../../entity/shared/constants';
import { useRemoveTagMutation, useRemoveTermMutation } from '../../../graphql/mutations.generated';
import { DomainLink } from './DomainLink';
import { TagProfileDrawer } from './TagProfileDrawer';

type Props = {
    uneditableTags?: GlobalTags | null;
    editableTags?: GlobalTags | null;
    editableGlossaryTerms?: GlossaryTerms | null;
    uneditableGlossaryTerms?: GlossaryTerms | null;
    domain?: Domain | null;
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
    refetch?: () => Promise<any>;

    proposedGlossaryTerms?: ActionRequest[];
    proposedTags?: ActionRequest[];
};

const TagWrapper = styled.div`
    margin-bottom: -8px;
`;

const TagLink = styled(Link)`
    display: inline-block;
    margin-bottom: 8px;
`;

const NoElementButton = styled(Button)`
    :not(:last-child) {
        margin-right: 8px;
    }
`;

const ProposedTerm = styled(Tag)`
    opacity: 0.7;
    border-style: dashed;
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
    proposedGlossaryTerms,
    proposedTags,
    domain,
    entityUrn,
    entityType,
    entitySubresource,
    refetch,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const [showAddModal, setShowAddModal] = useState(false);
    const [addModalType, setAddModalType] = useState(EntityType.Tag);

    const tagsEmpty =
        !editableTags?.tags?.length &&
        !uneditableTags?.tags?.length &&
        !editableGlossaryTerms?.terms?.length &&
        !uneditableGlossaryTerms?.terms?.length &&
        !proposedTags?.length &&
        !proposedGlossaryTerms?.length;
    const [removeTagMutation] = useRemoveTagMutation();
    const [removeTermMutation] = useRemoveTermMutation();
    const [tagProfileDrawerVisible, setTagProfileDrawerVisible] = useState(false);
    const [addTagUrn, setAddTagUrn] = useState('');

    const removeTag = (urnToRemove: string) => {
        onOpenModal?.();
        const tagToRemove = editableTags?.tags?.find((tag) => tag.tag.urn === urnToRemove);
        Modal.confirm({
            title: `Do you want to remove ${tagToRemove?.tag.name} tag?`,
            content: `Are you sure you want to remove the ${tagToRemove?.tag.name} tag?`,
            onOk() {
                if (entityUrn) {
                    removeTagMutation({
                        variables: {
                            input: {
                                tagUrn: urnToRemove,
                                resourceUrn: entityUrn,
                                subResource: entitySubresource,
                                subResourceType: entitySubresource ? SubResourceType.DatasetField : null,
                            },
                        },
                    })
                        .then(({ errors }) => {
                            if (!errors) {
                                message.success({ content: 'Removed Tag!', duration: 2 });
                            }
                        })
                        .then(refetch)
                        .catch((e) => {
                            message.destroy();
                            message.error({ content: `Failed to remove tag: \n ${e.message || ''}`, duration: 3 });
                        });
                }
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const removeTerm = (urnToRemove: string) => {
        onOpenModal?.();
        const termToRemove = editableGlossaryTerms?.terms?.find((term) => term.term.urn === urnToRemove);
        Modal.confirm({
            title: `Do you want to remove ${termToRemove?.term.name} term?`,
            content: `Are you sure you want to remove the ${termToRemove?.term.name} term?`,
            onOk() {
                if (entityUrn) {
                    removeTermMutation({
                        variables: {
                            input: {
                                termUrn: urnToRemove,
                                resourceUrn: entityUrn,
                                subResource: entitySubresource,
                                subResourceType: entitySubresource ? SubResourceType.DatasetField : null,
                            },
                        },
                    })
                        .then(({ errors }) => {
                            if (!errors) {
                                message.success({ content: 'Removed Term!', duration: 2 });
                            }
                        })
                        .then(refetch)
                        .catch((e) => {
                            message.destroy();
                            message.error({ content: `Failed to remove term: \n ${e.message || ''}`, duration: 3 });
                        });
                }
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    let renderedTags = 0;

    const showTagProfileDrawer = (urn: string) => {
        setTagProfileDrawerVisible(true);
        setAddTagUrn(urn);
    };

    const closeTagProfileDrawer = () => {
        setTagProfileDrawerVisible(false);
    };

    return (
        <TagWrapper>
            {domain && (
                <DomainLink urn={domain.urn} name={entityRegistry.getDisplayName(EntityType.Domain, domain) || ''} />
            )}
            {uneditableGlossaryTerms?.terms?.map((term) => (
                <TagLink to={entityRegistry.getEntityUrl(EntityType.GlossaryTerm, term.term.urn)} key={term.term.urn}>
                    <Tag closable={false}>
                        <BookOutlined style={{ marginRight: '3%' }} />
                        {entityRegistry.getDisplayName(EntityType.GlossaryTerm, term.term)}
                    </Tag>
                </TagLink>
            ))}
            {editableGlossaryTerms?.terms?.map((term) => (
                <TagLink to={entityRegistry.getEntityUrl(EntityType.GlossaryTerm, term.term.urn)} key={term.term.urn}>
                    <Tag
                        closable={canRemove}
                        onClose={(e) => {
                            e.preventDefault();
                            removeTerm(term.term.urn);
                        }}
                    >
                        <BookOutlined style={{ marginRight: '3%' }} />
                        {entityRegistry.getDisplayName(EntityType.GlossaryTerm, term.term)}
                    </Tag>
                </TagLink>
            ))}
            {proposedGlossaryTerms?.map((actionRequest) => (
                <TagLink
                    to={`/${entityRegistry.getPathName(EntityType.GlossaryTerm)}/${
                        actionRequest.params?.glossaryTermProposal?.glossaryTerm.urn
                    }`}
                    key={actionRequest.params?.glossaryTermProposal?.glossaryTerm.urn}
                >
                    <Tooltip overlay="Pending approval from owners">
                        <ProposedTerm
                            closable={false}
                            data-testid={`proposed-term-${actionRequest.params?.glossaryTermProposal?.glossaryTerm.name}`}
                        >
                            <BookOutlined style={{ marginRight: '3%' }} />
                            {actionRequest.params?.glossaryTermProposal?.glossaryTerm.name}
                            <ClockCircleOutlined style={{ color: 'orange', marginLeft: '3%' }} />
                        </ProposedTerm>
                    </Tooltip>
                </TagLink>
            ))}
            {/* uneditable tags are provided by ingestion pipelines exclusively */}
            {uneditableTags?.tags?.map((tag) => {
                renderedTags += 1;
                if (maxShow && renderedTags > maxShow) return null;
                return (
                    <TagLink to={entityRegistry.getEntityUrl(EntityType.Tag, tag?.tag?.urn)} key={tag?.tag?.urn}>
                        <StyledTag $colorHash={tag?.tag?.urn} $color={tag?.tag?.properties?.colorHex} closable={false}>
                            {entityRegistry.getDisplayName(EntityType.Tag, tag.tag)}
                        </StyledTag>
                    </TagLink>
                );
            })}
            {/* editable tags may be provided by ingestion pipelines or the UI */}
            {editableTags?.tags?.map((tag) => {
                renderedTags += 1;
                if (maxShow && renderedTags > maxShow) return null;
                return (
                    <StyledTag
                        style={{ cursor: 'pointer' }}
                        onClick={() => showTagProfileDrawer(tag?.tag?.urn)}
                        $colorHash={tag?.tag?.urn}
                        $color={tag?.tag?.properties?.colorHex}
                        closable={canRemove}
                        onClose={(e) => {
                            e.preventDefault();
                            removeTag(tag?.tag?.urn);
                        }}
                    >
                        {tag?.tag?.name}
                    </StyledTag>
                );
            })}
            {proposedTags?.map((actionRequest) => (
                <TagLink
                    to={`/${entityRegistry.getPathName(EntityType.Tag)}/${
                        actionRequest?.params?.tagProposal?.tag?.urn
                    }`}
                    key={actionRequest?.params?.tagProposal?.tag?.urn}
                >
                    <Tooltip overlay="Pending approval from owners">
                        <StyledTag
                            data-testid={`proposed-tag-${actionRequest?.params?.tagProposal?.tag?.name}`}
                            disabled
                            $colorHash={actionRequest?.params?.tagProposal?.tag?.urn}
                            $color={actionRequest?.params?.tagProposal?.tag?.properties?.colorHex}
                        >
                            {actionRequest?.params?.tagProposal?.tag?.name}
                            <ClockCircleOutlined style={{ color: 'orange', marginLeft: '3%' }} />
                        </StyledTag>
                    </Tooltip>
                </TagLink>
            ))}
            {tagProfileDrawerVisible && (
                <TagProfileDrawer
                    closeTagProfileDrawer={closeTagProfileDrawer}
                    tagProfileDrawerVisible={tagProfileDrawerVisible}
                    urn={addTagUrn}
                />
            )}
            {showEmptyMessage && canAddTag && tagsEmpty && (
                <Typography.Paragraph type="secondary">
                    {EMPTY_MESSAGES.tags.title}. {EMPTY_MESSAGES.tags.description}
                </Typography.Paragraph>
            )}
            {showEmptyMessage && canAddTerm && tagsEmpty && (
                <Typography.Paragraph type="secondary">
                    {EMPTY_MESSAGES.terms.title}. {EMPTY_MESSAGES.terms.description}
                </Typography.Paragraph>
            )}
            {canAddTag && (uneditableTags?.tags?.length || 0) + (editableTags?.tags?.length || 0) < 10 && (
                <NoElementButton
                    type={showEmptyMessage && tagsEmpty ? 'default' : 'text'}
                    onClick={() => {
                        setAddModalType(EntityType.Tag);
                        setShowAddModal(true);
                    }}
                    {...buttonProps}
                >
                    <PlusOutlined />
                    <span>Add Tag</span>
                </NoElementButton>
            )}
            {canAddTerm &&
                (uneditableGlossaryTerms?.terms?.length || 0) + (editableGlossaryTerms?.terms?.length || 0) < 10 && (
                    <NoElementButton
                        type={showEmptyMessage && tagsEmpty ? 'default' : 'text'}
                        onClick={() => {
                            setAddModalType(EntityType.GlossaryTerm);
                            setShowAddModal(true);
                        }}
                        {...buttonProps}
                    >
                        <PlusOutlined />
                        <span>Add Term</span>
                    </NoElementButton>
                )}
            {showAddModal && !!entityUrn && !!entityType && (
                <AddTagTermModal
                    type={addModalType}
                    globalTags={editableTags}
                    glossaryTerms={editableGlossaryTerms}
                    visible
                    onClose={() => {
                        onOpenModal?.();
                        setShowAddModal(false);
                        setTimeout(() => refetch?.(), 2000);
                    }}
                    entityUrn={entityUrn}
                    entityType={entityType}
                    entitySubresource={entitySubresource}
                />
            )}
        </TagWrapper>
    );
}
