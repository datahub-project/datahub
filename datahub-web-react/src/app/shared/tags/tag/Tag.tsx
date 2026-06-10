import { Modal, message } from 'antd';
import React, { useState } from 'react';
import Highlight from 'react-highlighter';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { StyledTag } from '@app/entity/shared/components/styled/StyledTag';
import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import { useHasMatchedFieldByUrn } from '@app/search/context/SearchResultContext';
import { TagProfileDrawer } from '@app/shared/tags/TagProfileDrawer';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useRemoveTagMutation } from '@graphql/mutations.generated';
import { EntityType, SubResourceType, TagAssociation } from '@types';

const TagLink = styled.span`
    display: inline-block;
    margin-bottom: 8px;
`;

interface Props {
    tag: TagAssociation;
    entityUrn?: string;
    entitySubresource?: string;
    canRemove?: boolean;
    readOnly?: boolean;
    highlightText?: string;
    onOpenModal?: () => void;
    refetch?: () => Promise<any>;
    fontSize?: number;
}

export default function Tag({
    tag,
    entityUrn,
    entitySubresource,
    canRemove,
    readOnly,
    highlightText,
    onOpenModal,
    refetch,
    fontSize,
}: Props) {
    const { t } = useTranslation('shared.tags');
    const { t: tc } = useTranslation('common.actions');
    const theme = useTheme();
    const highlightMatchStyle = { background: theme.colors.bgHighlight, padding: '0' };
    const entityRegistry = useEntityRegistry();
    const [removeTagMutation] = useRemoveTagMutation();
    const highlightTag = useHasMatchedFieldByUrn(tag.tag.urn, 'tags');

    const [tagProfileDrawerVisible, setTagProfileDrawerVisible] = useState(false);
    const [addTagUrn, setAddTagUrn] = useState('');

    const showTagProfileDrawer = (urn: string) => {
        if (!readOnly) {
            setTagProfileDrawerVisible(true);
            setAddTagUrn(urn);
        }
    };

    const closeTagProfileDrawer = () => {
        setTagProfileDrawerVisible(false);
    };

    const removeTag = (tagAssociationToRemove: TagAssociation) => {
        const tagToRemove = tagAssociationToRemove.tag;
        onOpenModal?.();
        Modal.confirm({
            title: t('removeTagConfirmTitle', { name: tagToRemove?.name }),
            content: t('removeTagConfirmContent', { name: tagToRemove?.name }),
            onOk() {
                if (tagAssociationToRemove.associatedUrn || entityUrn) {
                    removeTagMutation({
                        variables: {
                            input: {
                                tagUrn: tagToRemove.urn,
                                resourceUrn: tagAssociationToRemove.associatedUrn || entityUrn || '',
                                subResource: entitySubresource,
                                subResourceType: entitySubresource ? SubResourceType.DatasetField : null,
                            },
                        },
                    })
                        .then(({ errors }) => {
                            if (!errors) {
                                message.success({ content: t('removeTagSuccess'), duration: 2 });
                            }
                        })
                        .then(refetch)
                        .catch((e) => {
                            message.destroy();
                            message.error({ content: t('removeTagError', { error: e.message || '' }), duration: 3 });
                        });
                }
            },
            onCancel() {},
            okText: tc('yes'),
            maskClosable: true,
            closable: true,
        });
    };

    const displayName = entityRegistry.getDisplayName(EntityType.Tag, tag.tag);

    return (
        <>
            <HoverEntityTooltip entity={tag.tag}>
                <TagLink data-testid={`tag-${displayName}`}>
                    <StyledTag
                        style={{ cursor: 'pointer' }}
                        onClick={() => showTagProfileDrawer(tag?.tag?.urn)}
                        $colorHash={tag?.tag?.urn}
                        $color={tag?.tag?.properties?.colorHex}
                        closable={canRemove && !readOnly}
                        onClose={(e) => {
                            e.preventDefault();
                            removeTag(tag);
                        }}
                        fontSize={fontSize}
                        highlightTag={highlightTag}
                    >
                        <Highlight
                            style={{ marginLeft: 0, fontSize }}
                            matchStyle={highlightMatchStyle}
                            search={highlightText}
                        >
                            {displayName}
                        </Highlight>
                    </StyledTag>
                </TagLink>
            </HoverEntityTooltip>
            {tagProfileDrawerVisible && (
                <TagProfileDrawer
                    closeTagProfileDrawer={closeTagProfileDrawer}
                    tagProfileDrawerVisible={tagProfileDrawerVisible}
                    urn={addTagUrn}
                />
            )}
        </>
    );
}
