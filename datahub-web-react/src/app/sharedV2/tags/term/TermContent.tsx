import { ThunderboltOutlined } from '@ant-design/icons';
import { message, Modal, Tag } from 'antd';
import React from 'react';
import Highlight from 'react-highlighter';
import styled from 'styled-components';
import CloseIcon from '@mui/icons-material/Close';
import { useRemoveTermMutation } from '../../../../graphql/mutations.generated';
import { EntityType, GlossaryTermAssociation, SubResourceType } from '../../../../types.generated';
import { REDESIGN_COLORS } from '../../../entityV2/shared/constants';
import { useHasMatchedFieldByUrn } from '../../../search/context/SearchResultContext';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { generateColorFromPalette } from '../../../glossaryV2/colorUtils';
import LabelPropagationDetails from '../../propagation/LabelPropagationDetails';

const PROPAGATOR_URN = 'urn:li:corpuser:__datahub_propagator';

const highlightMatchStyle = { background: '#ffe58f', padding: '0' };

const TermContainer = styled.div`
    position: relative;
    max-width: 200px;

    .ant-tag.ant-tag {
        border-radius: 5px;
        border: 1px solid #ccd1dd;
    }

    :hover {
        .ant-tag.ant-tag {
            border: 1px solid ${REDESIGN_COLORS.TITLE_PURPLE};
        }
    }
`;

const StyledTerm = styled(Tag)<{ fontSize?: number; highlightTerm?: boolean; showOneAndCount?: boolean }>`
    &&& {
        ${(props) =>
            props.highlightTerm &&
            `
                background: ${props.theme.styles['highlight-color']};
                border: 1px solid ${props.theme.styles['highlight-border-color']};
            `}
    }
    ${(props) => props.fontSize && `font-size: ${props.fontSize}px;`}
    color: ${REDESIGN_COLORS.TEXT_HEADING};
    font-weight: 400;
    padding: 3px 8px;
    margin-right: 0;

    display: flex;
    position: relative;
    overflow: hidden;

    ${(props) =>
        props.showOneAndCount &&
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

const CloseButtonContainer = styled.div`
    display: none;
    position: absolute;
    top: -10px;
    right: -10px;
    background-color: ${REDESIGN_COLORS.TITLE_PURPLE};
    align-items: center;
    border-radius: 100%;
    padding: 5px;

    ${TermContainer}:hover & {
        display: flex;
    }
`;

const CloseIconStyle = styled(CloseIcon)`
    font-size: 10px !important;
    color: white;
`;

export const TermRibbon = styled.span<{ color: string; opacity?: number }>`
    position: absolute;
    left: -20px;
    top: 4px;
    width: 50px;
    transform: rotate(-45deg);
    padding: 4px;
    opacity: ${(props) => props.opacity || '1'};
    background-color: ${(props) => `${props.color}`};
`;

const StyledHighlight = styled(Highlight)`
    margin-left: 8px;
    overflow: hidden;
    text-overflow: ellipsis;
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
    context?: string | null;
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
    context,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const [removeTermMutation] = useRemoveTermMutation();
    const { parentNodes, urn, type } = term.term;

    const highlightTerm = useHasMatchedFieldByUrn(urn, 'glossaryTerms');
    const lastParentNode = parentNodes && parentNodes.count > 0 && parentNodes.nodes[parentNodes.count - 1];
    const termColor = lastParentNode
        ? lastParentNode.displayProperties?.colorHex || generateColorFromPalette(lastParentNode.urn)
        : generateColorFromPalette(urn);
    const displayName = entityRegistry.getDisplayName(type, term.term);
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
        <TermContainer>
            <StyledTerm
                style={{ cursor: 'pointer' }}
                fontSize={fontSize}
                highlightTerm={highlightTerm}
                showOneAndCount={showOneAndCount}
            >
                <TermRibbon color={termColor} />

                <StyledHighlight matchStyle={highlightMatchStyle} search={highlightText}>
                    {displayName}
                </StyledHighlight>
                <LabelPropagationDetails entityType={EntityType.GlossaryTerm} context={context} />

                {term.actor?.urn === PROPAGATOR_URN && <PropagateThunderbolt />}
            </StyledTerm>
            {canRemove && !readOnly && (
                <CloseButtonContainer
                    onClick={(e) => {
                        e.preventDefault();
                        removeTerm(term);
                    }}
                >
                    <CloseIconStyle />
                </CloseButtonContainer>
            )}
        </TermContainer>
    );
}
