import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { EntityType, GlossaryTermAssociation } from '../../../../types.generated';
import { HoverEntityTooltip } from '../../../recommendations/renderer/component/HoverEntityTooltip';
import { useEntityRegistry } from '../../../useEntityRegistry';
import TermContent from './TermContent';

const TermLink = styled(Link)`
    display: inline-block;
    margin-bottom: 8px;
`;

const TermWrapper = styled.span`
    display: inline-block;
    margin-bottom: 8px;
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
}

export default function StyledTerm(props: Props) {
    const { term, readOnly } = props;
    const entityRegistry = useEntityRegistry();

    if (readOnly) {
        return (
            <HoverEntityTooltip entity={term.term}>
                <TermWrapper>
                    <TermContent {...props} />
                </TermWrapper>
            </HoverEntityTooltip>
        );
    }

    return (
        <HoverEntityTooltip entity={term.term}>
            <TermLink to={entityRegistry.getEntityUrl(EntityType.GlossaryTerm, term.term.urn)} key={term.term.urn}>
                <TermContent {...props} />
            </TermLink>
        </HoverEntityTooltip>
    );
}
