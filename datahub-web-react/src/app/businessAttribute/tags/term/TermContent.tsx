import { toast } from '@components';
import { BookmarkSimple } from '@phosphor-icons/react/dist/csr/BookmarkSimple';
import { Lightning } from '@phosphor-icons/react/dist/csr/Lightning';
import { Tag } from 'antd';
import React, { useState } from 'react';
import Highlight from 'react-highlighter';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { useHasMatchedFieldByUrn } from '@app/search/context/SearchResultContext';
import { StopPropagation } from '@app/sharedV2/StopPropagation';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useRemoveTermMutation } from '@graphql/mutations.generated';
import { EntityType, GlossaryTermAssociation, SubResourceType } from '@types';

const PROPAGATOR_URN = 'urn:li:corpuser:__datahub_propagator';

const StyledTag = styled(Tag)<{ fontSize?: number; $highlightTerm?: boolean; $showOneAndCount?: boolean }>`
    &&& {
        ${(props) =>
            props.$highlightTerm &&
            `
                background: ${props.theme.colors.bgSurfaceBrand};
                border: 1px solid ${props.theme.colors.borderBrand};
            `}
    }
    ${(props) => props.fontSize && `font-size: ${props.fontSize}px;`}
    color: ${(props) => props.theme.colors.textSecondary};
    font-weight: 400;
    ${(props) =>
        props.$showOneAndCount &&
        `
            width: 100%;
            max-width: max-content;
            overflow: hidden;
            text-overflow: ellipsis;
            vertical-align: middle;
        `}
`;

const PropagateThunderbolt = styled(Lightning)`
    color: ${(props) => props.theme.colors.textSuccess};
    margin-right: -4px;
    font-weight: bold;
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
    refetch,
    showOneAndCount,
}: Props) {
    const { t } = useTranslation('shared.tags');
    const { t: tc } = useTranslation('common.actions');
    const theme = useTheme();
    const highlightMatchStyle = { background: theme.colors.bgHighlight, padding: '0' };
    const entityRegistry = useEntityRegistry();
    const [removeTermMutation] = useRemoveTermMutation();
    const highlightTerm = useHasMatchedFieldByUrn(term.term.urn, 'glossaryTerms');

    const [isRemoveModalOpen, setIsRemoveModalOpen] = useState(false);
    const termName = entityRegistry.getDisplayName(term.term.type, term.term);

    const removeTerm = () => {
        onOpenModal?.();
        setIsRemoveModalOpen(true);
    };

    const confirmRemoveTerm = () => {
        setIsRemoveModalOpen(false);
        if (term.associatedUrn || entityUrn) {
            removeTermMutation({
                variables: {
                    input: {
                        termUrn: term.term.urn,
                        resourceUrn: term.associatedUrn || entityUrn || '',
                        subResource: entitySubresource,
                        subResourceType: entitySubresource ? SubResourceType.DatasetField : null,
                    },
                },
            })
                .then(({ errors }) => {
                    if (!errors) {
                        toast.success(t('removeTermSuccess'), { duration: 2 });
                    }
                })
                .then(refetch)
                .catch((e) => {
                    toast.destroy();
                    toast.error(t('removeTermError', { error: e.message || '' }), { duration: 3 });
                });
        }
    };

    return (
        <>
            <StyledTag
                style={{ cursor: 'pointer' }}
                closable={canRemove && !readOnly}
                onClose={(e) => {
                    e.preventDefault();
                    removeTerm();
                }}
                fontSize={fontSize}
                $highlightTerm={highlightTerm}
                $showOneAndCount={showOneAndCount}
            >
                <BookmarkSimple
                    style={{ fill: theme.colors.icon, marginRight: '4px', marginBottom: 4, verticalAlign: 'middle' }}
                />
                <Highlight style={{ marginLeft: 0 }} matchStyle={highlightMatchStyle} search={highlightText}>
                    {entityRegistry.getDisplayName(EntityType.GlossaryTerm, term.term)}
                </Highlight>
                {term.actor?.urn === PROPAGATOR_URN && <PropagateThunderbolt />}
            </StyledTag>
            {isRemoveModalOpen && (
                <StopPropagation>
                    <ConfirmationModal
                        isOpen
                        handleClose={() => setIsRemoveModalOpen(false)}
                        handleConfirm={confirmRemoveTerm}
                        modalTitle={t('removeTermConfirmTitle', { name: termName })}
                        modalText={t('removeTermConfirmContent', { name: termName })}
                        confirmButtonText={tc('yes')}
                    />
                </StopPropagation>
            )}
        </>
    );
}
