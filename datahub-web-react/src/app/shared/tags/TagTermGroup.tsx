import { Modal, Tag, Typography, Button } from 'antd';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { BookOutlined, PlusOutlined } from '@ant-design/icons';

import { useEntityRegistry } from '../../useEntityRegistry';
import { EntityType, GlobalTags, GlobalTagsUpdate, GlossaryTerms } from '../../../types.generated';
import { convertTagsForUpdate } from './utils/convertTagsForUpdate';
import AddTagModal from './AddTagModal';
import { StyledTag } from '../../entity/shared/components/styled/StyledTag';
import { EMPTY_MESSAGES } from '../../entity/shared/constants';

type Props = {
    uneditableTags?: GlobalTags | null;
    editableTags?: GlobalTags | null;
    glossaryTerms?: GlossaryTerms | null;
    canRemove?: boolean;
    canAdd?: boolean;
    showEmptyMessage?: boolean;
    buttonProps?: Record<string, unknown>;
    updateTags?: (update: GlobalTagsUpdate) => Promise<any>;
    onOpenModal?: () => void;
    maxShow?: number;
};

const TagWrapper = styled.div`
    margin-bottom: -8px;
`;

const TagLink = styled(Link)`
    display: inline-block;
    margin-bottom: 8px;
`;

export default function TagTermGroup({
    uneditableTags,
    editableTags,
    canRemove,
    canAdd,
    showEmptyMessage,
    buttonProps,
    updateTags,
    onOpenModal,
    maxShow,
    glossaryTerms,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const [showAddModal, setShowAddModal] = useState(false);
    const tagsEmpty = !editableTags?.tags?.length;

    const removeTag = (urnToRemove: string) => {
        onOpenModal?.();
        const tagToRemove = editableTags?.tags?.find((tag) => tag.tag.urn === urnToRemove);
        const newTags = editableTags?.tags?.filter((tag) => tag.tag.urn !== urnToRemove);
        Modal.confirm({
            title: `Do you want to remove ${tagToRemove?.tag.name} tag?`,
            content: `Are you sure you want to remove the ${tagToRemove?.tag.name} tag?`,
            onOk() {
                updateTags?.({ tags: convertTagsForUpdate(newTags || []) });
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    let renderedTags = 0;

    return (
        <TagWrapper>
            {glossaryTerms?.terms?.map((term) => (
                <TagLink
                    to={`/${entityRegistry.getPathName(EntityType.GlossaryTerm)}/${term.term.urn}`}
                    key={term.term.urn}
                >
                    <Tag closable={false}>
                        {term.term.name}
                        <BookOutlined style={{ marginLeft: '2%' }} />
                    </Tag>
                </TagLink>
            ))}
            {/* uneditable tags are provided by ingestion pipelines exclusively */}
            {uneditableTags?.tags?.map((tag) => {
                renderedTags += 1;
                if (maxShow && renderedTags > maxShow) return null;
                return (
                    <TagLink to={`/${entityRegistry.getPathName(EntityType.Tag)}/${tag.tag.urn}`} key={tag.tag.urn}>
                        <StyledTag closable={false}>{tag.tag.name}</StyledTag>
                    </TagLink>
                );
            })}
            {/* editable tags may be provided by ingestion pipelines or the UI */}
            {editableTags?.tags?.map((tag) => {
                renderedTags += 1;
                if (maxShow && renderedTags > maxShow) return null;
                return (
                    <TagLink to={`/${entityRegistry.getPathName(EntityType.Tag)}/${tag.tag.urn}`} key={tag.tag.urn}>
                        <StyledTag
                            $colorHash={tag.tag.urn}
                            closable={canRemove}
                            onClose={(e) => {
                                e.preventDefault();
                                removeTag(tag.tag.urn);
                            }}
                        >
                            {tag.tag.name}
                        </StyledTag>
                    </TagLink>
                );
            })}
            {showEmptyMessage && canAdd && tagsEmpty && (
                <Typography.Paragraph type="secondary">
                    {EMPTY_MESSAGES.tags.title}. {EMPTY_MESSAGES.tags.description}
                </Typography.Paragraph>
            )}
            {canAdd && (uneditableTags?.tags?.length || 0) + (editableTags?.tags?.length || 0) < 10 && (
                <>
                    <Button
                        type={showEmptyMessage && tagsEmpty ? 'default' : 'text'}
                        onClick={() => setShowAddModal(true)}
                        {...buttonProps}
                    >
                        <PlusOutlined />
                        Add Tag
                    </Button>
                    {showAddModal && (
                        <AddTagModal
                            globalTags={editableTags}
                            updateTags={updateTags}
                            visible
                            onClose={() => {
                                onOpenModal?.();
                                setShowAddModal(false);
                            }}
                        />
                    )}
                </>
            )}
        </TagWrapper>
    );
}
