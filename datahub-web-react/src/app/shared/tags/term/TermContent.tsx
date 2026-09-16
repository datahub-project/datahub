import { BookmarkSimple } from '@phosphor-icons/react/dist/csr/BookmarkSimple';
import { Modal, Tag, message } from 'antd';
import React from 'react';
import Highlight from 'react-highlighter';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { useHasMatchedFieldByUrn } from '@app/search/context/SearchResultContext';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useRemoveTermMutation } from '@graphql/mutations.generated';
import { EntityType, GlossaryTermAssociation, SubResourceType } from '@types';

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

    const removeTerm = (termToRemove: GlossaryTermAssociation) => {
        onOpenModal?.();
        const termName = termToRemove && entityRegistry.getDisplayName(termToRemove.term.type, termToRemove.term);
        Modal.confirm({
            title: t('removeTermConfirmTitle', { name: termName }),
            content: t('removeTermConfirmContent', { name: termName }),
            onOk() {
                if (termToRemove.associatedUrn || entityUrn) {
                    removeTermMutation({
                        variables: {
                            input: {
                                termUrn: termToRemove.term.urn,
                                resourceUrn: termToRemove.associatedUrn || entityUrn || '',
                                subResource: entitySubresource,
                                subResourceType: entitySubresource ? SubResourceType.DatasetField : null,
                            },
                        },
                    })
                        .then(({ errors }) => {
                            if (!errors) {
                                message.success({ content: t('removeTermSuccess'), duration: 2 });
                            }
                        })
                        .then(refetch)
                        .catch((e) => {
                            message.destroy();
                            message.error({ content: t('removeTermError', { error: e.message || '' }), duration: 3 });
                        });
                }
            },
            onCancel() {},
            okText: tc('yes'),
            maskClosable: true,
            closable: true,
        });
    };

    return (
        <StyledTag
            style={{ cursor: 'pointer' }}
            closable={canRemove && !readOnly}
            onClose={(e) => {
                e.preventDefault();
                removeTerm(term);
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
        </StyledTag>
    );
}
