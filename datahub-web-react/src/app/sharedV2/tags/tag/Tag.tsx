import { toast } from '@components';
import React, { useState } from 'react';
import Highlight from 'react-highlighter';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import { useHasMatchedFieldByUrn } from '@app/search/context/SearchResultContext';
import { TagProfileDrawer } from '@app/shared/tags/TagProfileDrawer';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import TagPill from '@app/sharedV2/tags/TagPill';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { useIsEmbeddedProfile } from '@src/app/shared/useEmbeddedProfileLinkProps';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

import { useRemoveTagMutation } from '@graphql/mutations.generated';
import { EntityType, SubResourceType, TagAssociation } from '@types';

const TagLink = styled.span<{ $showOneAndCount?: boolean; $shouldNotAddBottomMargin?: boolean }>`
    display: flex;
    cursor: pointer;
    margin-bottom: ${(props) => `${props.$shouldNotAddBottomMargin ? '0px' : '4px'}`};
    max-width: inherit;
    ${(props) =>
        props.$showOneAndCount &&
        `
 max-width: 200px;
 `}
`;

const StyledHighlight = styled(Highlight)<{ $maxWidth?: number }>`
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: ${(props) => (props.$maxWidth ? `${props.$maxWidth}px` : '120px')};
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
    showOneAndCount?: boolean;
    context?: string | null;
    maxWidth?: number;
    tagStyle?: React.CSSProperties;
    options?: {
        shouldNotOpenDrawerOnClick?: boolean;
        shouldNotAddBottomMargin?: boolean;
        shouldShowEllipses?: boolean;
    };
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
    maxWidth,
    tagStyle,
    options,
}: Props) {
    const theme = useTheme();
    const { t } = useTranslation('shared.tags');
    const entityRegistry = useEntityRegistry();
    const isEmbeddedProfile = useIsEmbeddedProfile();
    const [removeTagMutation] = useRemoveTagMutation();
    const highlightTag = useHasMatchedFieldByUrn(tag.tag.urn, 'tags');
    const highlightMatchStyle = { background: theme.colors.bgHighlight, padding: '0' };

    const [tagProfileDrawerVisible, setTagProfileDrawerVisible] = useState(false);
    const [addTagUrn, setAddTagUrn] = useState('');
    const [showConfirmDelete, setShowConfirmDelete] = useState(false);
    const displayName = entityRegistry.getDisplayName(EntityType.Tag, tag.tag);
    const previewContext = { propagationDetails: { context, attribution: tag.attribution } };

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
                        toast.success(t('removeTagSuccess'), { duration: 2 });
                    }
                })
                .then(refetch)
                .catch((e) => {
                    toast.error(t('removeTagError', { error: e.message || '' }), { duration: 3 });
                });
        }
    };

    const handleTagClick = () => {
        if (isEmbeddedProfile) {
            window.open(resolveRuntimePath(entityRegistry.getEntityUrl(EntityType.Tag, tag.tag.urn)), '_blank');
        } else if (!options?.shouldNotOpenDrawerOnClick) {
            showTagProfileDrawer(tag?.tag?.urn);
        }
    };

    return (
        <>
            <HoverEntityTooltip entity={tag.tag} width={250} previewContext={previewContext}>
                <TagLink
                    data-testid={`tag-${displayName}`}
                    $showOneAndCount={showOneAndCount}
                    $shouldNotAddBottomMargin={options?.shouldNotAddBottomMargin}
                    onClick={handleTagClick}
                    style={tagStyle}
                >
                    <TagPill
                        name={displayName}
                        color={tag?.tag?.properties?.colorHex}
                        colorHash={tag?.tag?.urn}
                        clickable
                        variant={highlightTag ? 'highlighted' : 'default'}
                        size={fontSize && fontSize <= 10 ? 'sm' : 'md'}
                        dataTestId={`tag-${displayName}-pill`}
                        onRemove={
                            canRemove && !readOnly
                                ? (e) => {
                                      e.preventDefault();
                                      e.stopPropagation();
                                      onOpenModal?.();
                                      setShowConfirmDelete(true);
                                  }
                                : undefined
                        }
                    >
                        <StyledHighlight $maxWidth={maxWidth} matchStyle={highlightMatchStyle} search={highlightText}>
                            {options?.shouldShowEllipses ? (
                                <span className="test-mini-preview-class">{displayName}</span>
                            ) : (
                                displayName
                            )}
                        </StyledHighlight>
                    </TagPill>
                </TagLink>
            </HoverEntityTooltip>
            {tagProfileDrawerVisible && (
                <TagProfileDrawer
                    closeTagProfileDrawer={closeTagProfileDrawer}
                    tagProfileDrawerVisible={tagProfileDrawerVisible}
                    urn={addTagUrn}
                />
            )}
            <ConfirmationModal
                isOpen={showConfirmDelete}
                handleClose={() => setShowConfirmDelete(false)}
                handleConfirm={() => removeTag(tag)}
                modalTitle={t('deleteTagTitle', { name: displayName })}
                modalText={t('removeTagConfirmation')}
            />
        </>
    );
}
