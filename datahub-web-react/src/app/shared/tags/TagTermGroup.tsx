import { Modal, Tag, Typography, Button, message } from 'antd';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { BookOutlined, PlusOutlined } from '@ant-design/icons';

import { useEntityRegistry } from '../../useEntityRegistry';
import { Domain, EntityType, GlobalTags, GlossaryTerms, SubResourceType } from '../../../types.generated';
import AddTagTermModal from './AddTagTermModal';
import { StyledTag } from '../../entity/shared/components/styled/StyledTag';
import { EMPTY_MESSAGES, ANTD_GRAY } from '../../entity/shared/constants';
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
};

const TagWrapper = styled.div`
    margin-bottom: -8px;
`;

const TermLink = styled(Link)`
    display: inline-block;
    margin-bottom: 8px;
`;

const TagLink = styled.span`
    display: inline-block;
    margin-bottom: 8px;
`;

const NoElementButton = styled(Button)`
    :not(:last-child) {
        margin-right: 8px;
    }
`;

const TagText = styled.span`
    color: ${ANTD_GRAY[7]};
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
    refetch,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const [showAddModal, setShowAddModal] = useState(false);
    const [addModalType, setAddModalType] = useState(EntityType.Tag);
    const tagsEmpty =
        !editableTags?.tags?.length &&
        !uneditableTags?.tags?.length &&
        !editableGlossaryTerms?.terms?.length &&
        !uneditableGlossaryTerms?.terms?.length;
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
            {uneditableGlossaryTerms?.terms?.map((term) => {
                renderedTags += 1;
                if (maxShow && renderedTags === maxShow + 1)
                    return (
                        <TagText>
                            {uneditableGlossaryTerms?.terms
                                ? `+${uneditableGlossaryTerms?.terms?.length - maxShow}`
                                : null}
                        </TagText>
                    );
                if (maxShow && renderedTags > maxShow) return null;

                return (
                    <TermLink
                        to={entityRegistry.getEntityUrl(EntityType.GlossaryTerm, term.term.urn)}
                        key={term.term.urn}
                    >
                        <Tag closable={false}>
                            <BookOutlined style={{ marginRight: '3%' }} />
                            {entityRegistry.getDisplayName(EntityType.GlossaryTerm, term.term)}
                        </Tag>
                    </TermLink>
                );
            })}
            {editableGlossaryTerms?.terms?.map((term) => (
                <TermLink to={entityRegistry.getEntityUrl(EntityType.GlossaryTerm, term.term.urn)} key={term.term.urn}>
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
                </TermLink>
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
                    <TagLink key={tag?.tag?.urn}>
                        <StyledTag
                            onClick={() => showTagProfileDrawer(tag?.tag?.urn)}
                            $colorHash={tag?.tag?.urn}
                            $color={tag?.tag?.properties?.colorHex}
                            closable={false}
                        >
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
                    <TagLink>
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
                    </TagLink>
                );
            })}
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
                        refetch?.();
                    }}
                    entityUrn={entityUrn}
                    entityType={entityType}
                    entitySubresource={entitySubresource}
                />
            )}
        </TagWrapper>
    );
}
