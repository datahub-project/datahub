import { message, Modal } from 'antd';
import styled from 'styled-components';
import React, { useState } from 'react';
import Highlight from 'react-highlighter';
import { useIsEmbeddedProfile } from '@src/app/shared/useEmbeddedProfileLinkProps';
import { useRemoveTagMutation } from '../../../../graphql/mutations.generated';
import { EntityType, SubResourceType, TagAssociation } from '../../../../types.generated';
import { StyledTag } from '../../../entityV2/shared/components/styled/StyledTag';
import { HoverEntityTooltip } from '../../../recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { TagProfileDrawer } from '../../../shared/tags/TagProfileDrawer';
import { useHasMatchedFieldByUrn } from '../../../search/context/SearchResultContext';
import LabelPropagationDetails from '../../propagation/LabelPropagationDetails';

const TagLink = styled.span<{ $showOneAndCount?: boolean }>`
    display: inline-block;
    margin-bottom: 4px;
    ${(props) =>
        props.$showOneAndCount &&
        `
            width: 90%;
            max-width: max-content;
        `}
`;

const highlightMatchStyle = { background: '#ffe58f', padding: '0' };

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
    showOneAndCount?: boolean;
    context?: string | null;
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
    showOneAndCount,
    context,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const isEmbeddedProfile = useIsEmbeddedProfile();
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
            title: `Do you want to remove ${tagToRemove?.name} tag?`,
            content: `Are you sure you want to remove the ${tagToRemove?.name} tag?`,
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

    const displayName = entityRegistry.getDisplayName(EntityType.Tag, tag.tag);

    return (
        <>
            <HoverEntityTooltip entity={tag.tag} width={250}>
                <TagLink data-testid={`tag-${displayName}`} $showOneAndCount={showOneAndCount}>
                    <StyledTag
                        style={{ cursor: 'pointer', display: 'flex', alignItems: 'center' }}
                        onClick={() => {
                            if (isEmbeddedProfile) {
                                window.open(entityRegistry.getEntityUrl(EntityType.Tag, tag.tag.urn), '_blank');
                            } else {
                                showTagProfileDrawer(tag?.tag?.urn);
                            }
                        }}
                        $colorHash={tag?.tag?.urn}
                        $color={tag?.tag?.properties?.colorHex}
                        closable={canRemove && !readOnly}
                        onClose={(e) => {
                            e.preventDefault();
                            removeTag(tag);
                        }}
                        fontSize={fontSize}
                        $highlightTag={highlightTag}
                        $showOneAndCount={showOneAndCount}
                    >
                        <Highlight
                            style={{ marginLeft: 0, fontSize }}
                            matchStyle={highlightMatchStyle}
                            search={highlightText}
                        >
                            {displayName}
                        </Highlight>
                        <LabelPropagationDetails entityType={EntityType.Tag} context={context} />
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
