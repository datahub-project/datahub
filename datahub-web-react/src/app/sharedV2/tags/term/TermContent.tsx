import { toast } from '@components';
import { Lightning } from '@phosphor-icons/react/dist/csr/Lightning';
import React, { useState } from 'react';
import Highlight from 'react-highlighter';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import GlossaryTermPill from '@app/glossaryV2/GlossaryTermPill';
import { getGlossaryTermColor, useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { useHasMatchedFieldByUrn } from '@app/search/context/SearchResultContext';
import { StopPropagation } from '@app/shared/StopPropagation';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useRemoveTermMutation } from '@graphql/mutations.generated';
import { DataHubPageModuleType, GlossaryTermAssociation, SubResourceType } from '@types';

const PROPAGATOR_URN = 'urn:li:corpuser:__datahub_propagator';

const TermContainer = styled.div<{ $showOneAndCount?: boolean }>`
    position: relative;
    max-width: 200px;
    ${(props) =>
        props.$showOneAndCount &&
        `
            width: 100%;
            max-width: max-content;
            overflow: hidden;
            vertical-align: middle;
        `}
`;

const PropagateThunderbolt = styled(Lightning).attrs({ weight: 'fill', size: 14 })`
    color: ${(props) => props.theme.colors.iconSuccess};
    margin-right: -4px;
`;

const StyledHighlight = styled(Highlight)`
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

interface Props {
    term: GlossaryTermAssociation;
    entityUrn?: string;
    entitySubresource?: string;
    canRemove?: boolean;
    readOnly?: boolean;
    highlightText?: string;
    fontSize?: number;
    onOpenModal?: () => void;
    onCloseModal?: () => void;
    refetch?: () => Promise<any>;
    showOneAndCount?: boolean;
}

export default function TermContent({
    term,
    entityUrn,
    entitySubresource,
    canRemove,
    readOnly,
    highlightText,
    fontSize,
    onOpenModal,
    onCloseModal,
    refetch,
    showOneAndCount,
}: Props) {
    const { t } = useTranslation('shared.tags');
    const theme = useTheme();
    const entityRegistry = useEntityRegistry();
    const { reloadByKeyType } = useReloadableContext();
    const highlightMatchStyle = { background: theme.colors.bgHighlight, padding: '0' };
    const [removeTermMutation] = useRemoveTermMutation();
    const { urn, type } = term.term;
    const generateColor = useGenerateGlossaryColorFromPalette();
    const [termTobeRemoved, setTermToBeRemoved] = useState<GlossaryTermAssociation | null>(null);
    const termName = termTobeRemoved && entityRegistry.getDisplayName(termTobeRemoved.term.type, termTobeRemoved.term);
    const highlightTerm = useHasMatchedFieldByUrn(urn, 'glossaryTerms');
    const termColor = getGlossaryTermColor(term.term, generateColor);
    const displayName = entityRegistry.getDisplayName(type, term.term);

    const removeTerm = () => {
        if (termTobeRemoved?.associatedUrn || entityUrn) {
            removeTermMutation({
                variables: {
                    input: {
                        termUrn: termTobeRemoved?.term?.urn || '',
                        resourceUrn: termTobeRemoved?.associatedUrn || entityUrn || '',
                        subResource: entitySubresource,
                        subResourceType: entitySubresource ? SubResourceType.DatasetField : null,
                    },
                },
            })
                .then(({ errors }) => {
                    if (!errors) {
                        toast.success(t('removeTermSuccess'), { duration: 2 });
                        // Reload modules
                        // RelatedTerms - to update related terms in case some of them was removed
                        // ChildHierarchy - to update contents module in glossary node
                        reloadByKeyType(
                            [
                                getReloadableKeyType(
                                    ReloadableKeyTypeNamespace.MODULE,
                                    DataHubPageModuleType.RelatedTerms,
                                ),
                                getReloadableKeyType(
                                    ReloadableKeyTypeNamespace.MODULE,
                                    DataHubPageModuleType.ChildHierarchy,
                                ),
                            ],
                            3000,
                        );
                    }
                    setTermToBeRemoved(null);
                })
                .then(refetch)
                .catch((e) => {
                    toast.error(t('removeTermError', { error: e.message || '' }), { duration: 3 });
                });
        }
    };

    return (
        <TermContainer data-testid={`term-${displayName}`} $showOneAndCount={showOneAndCount}>
            <GlossaryTermPill
                name={displayName}
                color={termColor}
                clickable
                variant={highlightTerm ? 'highlighted' : 'default'}
                size={fontSize && fontSize <= 10 ? 'sm' : 'md'}
                // Pass an explicit `-pill` suffix so the inner pill's default `term-{name}`
                // testid doesn't collide with the outer `TermContainer` selector and trip
                // Playwright/Cypress strict-mode. Mirrors the `Tag` / `TagPill` pairing.
                dataTestId={`term-${displayName}-pill`}
                rightAdornment={term.actor?.urn === PROPAGATOR_URN ? <PropagateThunderbolt /> : undefined}
                onRemove={
                    canRemove && !readOnly
                        ? (e) => {
                              e.preventDefault();
                              e.stopPropagation();
                              onOpenModal?.();
                              setTermToBeRemoved(term);
                          }
                        : undefined
                }
            >
                <StyledHighlight matchStyle={highlightMatchStyle} search={highlightText}>
                    {displayName}
                </StyledHighlight>
            </GlossaryTermPill>
            <StopPropagation>
                <ConfirmationModal
                    isOpen={!!termTobeRemoved}
                    handleClose={() => {
                        setTermToBeRemoved(null);
                        onCloseModal?.();
                    }}
                    handleConfirm={removeTerm}
                    modalTitle={t('removeTermConfirmTitle', { name: termName })}
                    modalText={t('removeTermConfirmContent', { name: termName })}
                />
            </StopPropagation>
        </TermContainer>
    );
}
