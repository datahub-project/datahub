import { Text } from '@components';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import { Typography } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { EMPTY_MESSAGES } from '@app/entity/shared/constants';
import { isPropagated } from '@app/entity/shared/propagation/utils';
import AddTagTerm from '@app/sharedV2/tags/AddTagTerm';
import { DomainLink } from '@app/sharedV2/tags/DomainLink';
import Tag from '@app/sharedV2/tags/tag/Tag';
import Term from '@app/sharedV2/tags/term/Term';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Tooltip } from '@src/alchemy-components';
import { dedupeByUrn } from '@src/utils/dedupeByUrn';

import {
    Domain as DomainEntity,
    EntityType,
    GlobalTags,
    GlossaryTermAssociation,
    GlossaryTerms,
    TagAssociation,
} from '@types';

type Props = {
    directTags?: GlobalTags | null;
    uneditableTags?: GlobalTags | null;
    editableTags?: GlobalTags | null;
    directGlossaryTerms?: GlossaryTerms | null;
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

// A term/tag flattened from its source bucket, carrying the per-bucket render config: whether it can be
// removed, the subresource it belongs to, and whether it's managed (uneditable) elsewhere.
type OrderedTerm = {
    term: GlossaryTermAssociation;
    canRemove?: boolean;
    entitySubresource?: string;
    managed: boolean;
};

type OrderedTag = {
    tag: TagAssociation;
    canRemove?: boolean;
    entitySubresource?: string;
    managed: boolean;
};

const NoElementButton = styled.div`
    :not(:last-child) {
        margin-right: 8px;
    }

    margin: 0px;
    padding: 0px;
    flex-basis: 100%;
    color: ${(props) => props.theme.colors.text};

    :hover {
        cursor: pointer;
        color: ${(props) => props.theme.colors.hyperlinks};
    }
`;
const TagTermWrapper = styled.div<{ $showOneAndCount?: boolean }>`
    display: flex;
    flex-wrap: ${(props) => (!props.$showOneAndCount ? 'wrap' : '')};
    align-items: center;
    row-gap: 4px;
    column-gap: 4px;
    max-width: 100%;
`;

const TagText = styled.span`
    color: ${(props) => props.theme.colors.text};
    font-size: 10px;
    font-weight: 400;
    line-height: 8px;
`;

const StyledPlusIcon = styled(Plus).attrs({ size: 10, weight: 'bold' })`
    margin-right: 8px;
`;

const EmptyText = styled(Text)`
    && {
        margin-right: 8px;
    }
`;

const Count = styled(Typography.Text)`
    color: ${(props) => props.theme.colors.text};
    font-size: 12px;
    font-weight: 400;
    line-height: 24px;
    overflow: hidden;
    white-space: nowrap;
`;

const AddText = styled.span`
    color: ${(props) => props.theme.colors.text};
    font-size: 12px;
    font-weight: 500;
    line-height: 16px;

    :hover {
        color: ${(props) => props.theme.colors.hyperlinks};
    }
`;

// "+N" indicator for tags/terms hidden by showOneAndCount or maxShow. Renders nothing when none are hidden.
function OverflowCount({ hidden, showOneAndCount }: { hidden: number; showOneAndCount?: boolean }) {
    if (hidden <= 0) return null;
    return showOneAndCount ? <Count>{`+${hidden}`}</Count> : <TagText>{`+${hidden}`}</TagText>;
}

export default function TagTermGroup({
    directTags,
    uneditableTags,
    editableTags,
    canRemove,
    canAddTag,
    canAddTerm,
    showEmptyMessage,
    buttonProps,
    onOpenModal,
    maxShow,
    directGlossaryTerms,
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
    const { t } = useTranslation('shared.tags');
    const entityRegistry = useEntityRegistry();
    const [showAddModal, setShowAddModal] = useState(false);
    const [addModalType, setAddModalType] = useState(EntityType.Tag);

    const tagsEmpty = !directTags?.tags?.length && !editableTags?.tags?.length && !uneditableTags?.tags?.length;

    const termsEmpty =
        !directGlossaryTerms?.terms?.length &&
        !editableGlossaryTerms?.terms?.length &&
        !uneditableGlossaryTerms?.terms?.length;

    // Maintain order of uneditable -> direct -> editable
    const orderedTerms: OrderedTerm[] = [
        ...(uneditableGlossaryTerms?.terms ?? []).map((term) => ({
            term,
            canRemove: false,
            entitySubresource,
            managed: true,
        })),
        ...(directGlossaryTerms?.terms ?? []).map((term) => ({
            term,
            canRemove,
            entitySubresource: undefined,
            managed: false,
        })),
        ...(editableGlossaryTerms?.terms ?? []).map((term) => ({
            term,
            canRemove,
            entitySubresource,
            managed: false,
        })),
    ];
    const dedupedTerms = dedupeByUrn(
        orderedTerms,
        (item) => item.term.term.urn,
        (item) => isPropagated(item.term.attribution?.sourceDetail),
    );

    const orderedTags: OrderedTag[] = [
        ...(uneditableTags?.tags ?? []).map((tag) => ({
            tag,
            canRemove: false,
            entitySubresource,
            managed: true,
        })),
        ...(directTags?.tags ?? []).map((tag) => ({
            tag,
            canRemove,
            entitySubresource: undefined,
            managed: false,
        })),
        ...(editableTags?.tags ?? []).map((tag) => ({ tag, canRemove, entitySubresource, managed: false })),
    ];
    const dedupedTags = dedupeByUrn(
        orderedTags,
        (item) => item.tag.tag.urn,
        (item) => isPropagated(item.tag.attribution?.sourceDetail),
    );

    // Collapse to a single item (showOneAndCount) or cap at maxShow, otherwise render everything. Slicing here
    // keeps the "+N" overflow indicator out of the map and avoids walking the full list when collapsed.
    const visibleLimit = showOneAndCount ? 1 : maxShow;
    const visibleTerms = visibleLimit ? dedupedTerms.slice(0, visibleLimit) : dedupedTerms;
    const visibleTags = visibleLimit ? dedupedTags.slice(0, visibleLimit) : dedupedTags;

    return (
        <TagTermWrapper $showOneAndCount={showOneAndCount}>
            {domain && (
                <DomainLink domain={domain} name={entityRegistry.getDisplayName(EntityType.Domain, domain) || ''} />
            )}
            {visibleTerms.map((orderedTerm) => {
                const termElement = (
                    <Term
                        key={orderedTerm.term.term.urn}
                        term={orderedTerm.term}
                        entityUrn={entityUrn}
                        entitySubresource={orderedTerm.entitySubresource}
                        canRemove={orderedTerm.canRemove}
                        readOnly={readOnly}
                        highlightText={highlightText}
                        onOpenModal={onOpenModal}
                        refetch={refetch}
                        fontSize={fontSize}
                        showOneAndCount={showOneAndCount}
                    />
                );

                // Managed (uneditable) terms come from ingestion pipelines or v2 fields and can't be removed here
                if (orderedTerm.managed) {
                    return (
                        <Tooltip key={orderedTerm.term.term.urn} title={t('managedTermTooltip')}>
                            {termElement}
                        </Tooltip>
                    );
                }
                return termElement;
            })}
            <OverflowCount hidden={dedupedTerms.length - visibleTerms.length} showOneAndCount={showOneAndCount} />
            {visibleTags.map((orderedTag) => {
                const tagElement = (
                    <Tag
                        key={orderedTag.tag.tag.urn}
                        tag={orderedTag.tag}
                        entityUrn={entityUrn}
                        entitySubresource={orderedTag.entitySubresource}
                        canRemove={orderedTag.canRemove}
                        readOnly={readOnly}
                        highlightText={highlightText}
                        onOpenModal={onOpenModal}
                        refetch={refetch}
                        fontSize={fontSize}
                        showOneAndCount={showOneAndCount}
                    />
                );

                // Managed (uneditable) tags come from ingestion pipelines or v2 fields and can't be removed here
                if (orderedTag.managed) {
                    return (
                        <Tooltip key={orderedTag.tag.tag.urn} title={t('managedTagTooltip')}>
                            {tagElement}
                        </Tooltip>
                    );
                }
                return tagElement;
            })}
            <OverflowCount hidden={dedupedTags.length - visibleTags.length} showOneAndCount={showOneAndCount} />
            {showEmptyMessage &&
                canAddTag &&
                tagsEmpty /* eslint-disable i18next/no-literal-string -- empty-state text lives in EMPTY_MESSAGES constants (out of scope); only the trailing "." is local punctuation */ && (
                    <EmptyText type="span" color="textSecondary">
                        {EMPTY_MESSAGES.tags.title}.
                    </EmptyText>
                    /* eslint-enable i18next/no-literal-string */
                )}
            {showEmptyMessage &&
                canAddTerm &&
                termsEmpty /* eslint-disable i18next/no-literal-string -- empty-state text lives in EMPTY_MESSAGES constants (out of scope); only the trailing "." is local punctuation */ && (
                    <EmptyText type="span" color="textSecondary">
                        {EMPTY_MESSAGES.terms.title}.
                    </EmptyText>
                    /* eslint-enable i18next/no-literal-string */
                )}
            {canAddTag && !readOnly && showAddButton && (
                <NoElementButton
                    onClick={() => {
                        setAddModalType(EntityType.Tag);
                        setShowAddModal(true);
                    }}
                    {...buttonProps}
                >
                    <StyledPlusIcon />
                    <AddText data-testid="schema-field-add-tags-button">{t('addTagsButtonLower')}</AddText>
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
                    <StyledPlusIcon />
                    <AddText data-testid="schema-field-add-terms-button">{t('addTermsButtonLower')}</AddText>
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
