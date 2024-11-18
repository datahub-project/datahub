import { ThunderboltOutlined } from '@ant-design/icons';
import { message, Modal, Tag } from 'antd';
import React from 'react';
import Highlight from 'react-highlighter';
import styled from 'styled-components';
import { BookmarkSimple } from '@phosphor-icons/react';
import { useRemoveTermMutation } from '../../../../graphql/mutations.generated';
import { EntityType, GlossaryTermAssociation, SubResourceType } from '../../../../types.generated';
import { REDESIGN_COLORS } from '../../../entityV2/shared/constants';
import { useHasMatchedFieldByUrn } from '../../../search/context/SearchResultContext';
import { useEntityRegistry } from '../../../useEntityRegistry';

const PROPAGATOR_URN = 'urn:li:corpuser:__datahub_propagator';

const highlightMatchStyle = { background: '#ffe58f', padding: '0' };

const StyledTag = styled(Tag)<{ fontSize?: number; $highlightTerm?: boolean; $showOneAndCount?: boolean }>`
    &&& {
        ${(props) =>
            props.$highlightTerm &&
            `
                background: ${props.theme.styles['highlight-color']};
                border: 1px solid ${props.theme.styles['highlight-border-color']};
            `}
    }
    ${(props) => props.fontSize && `font-size: ${props.fontSize}px;`}
    color: ${REDESIGN_COLORS.DARK_GREY};
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

const PropagateThunderbolt = styled(ThunderboltOutlined)`
    color: rgba(0, 143, 100, 0.95);
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
    const entityRegistry = useEntityRegistry();
    const [removeTermMutation] = useRemoveTermMutation();
    const highlightTerm = useHasMatchedFieldByUrn(term.term.urn, 'glossaryTerms');

    const removeTerm = (termToRemove: GlossaryTermAssociation) => {
        onOpenModal?.();
        const termName = termToRemove && entityRegistry.getDisplayName(termToRemove.term.type, termToRemove.term);
        Modal.confirm({
            title: `Do you want to remove ${termName} term?`,
            content: `Are you sure you want to remove the ${termName} term?`,
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
            <BookmarkSimple style={{ fill: '#56668E', marginRight: '4px', marginBottom: 4, verticalAlign: 'middle' }} />
            <Highlight style={{ marginLeft: 0 }} matchStyle={highlightMatchStyle} search={highlightText}>
                {entityRegistry.getDisplayName(EntityType.GlossaryTerm, term.term)}
            </Highlight>
            {term.actor?.urn === PROPAGATOR_URN && <PropagateThunderbolt />}
        </StyledTag>
    );
}
