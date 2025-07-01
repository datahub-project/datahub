import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { HoverEntityTooltip } from '@app/recommendations/renderer/component/HoverEntityTooltip';
import { useEmbeddedProfileLinkProps } from '@app/shared/useEmbeddedProfileLinkProps';
import TermContent from '@app/sharedV2/tags/term/TermContent';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, GlossaryTermAssociation } from '@types';

const TermLink = styled(Link)<{ $showOneAndCount?: boolean }>`
    display: inline-block;
    max-width: inherit;
    ${(props) =>
        props.$showOneAndCount &&
        `
            width: 90%;
            max-width: max-content;
        `}
`;

const TermWrapper = styled.span<{ $showOneAndCount?: boolean }>`
    display: inline-block;
    max-width: inherit;
    ${(props) =>
        props.$showOneAndCount &&
        `
            width: 90%;
            max-width: max-content;
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
    context?: string | null;
}

export default function Term(props: Props) {
    const { term, readOnly, showOneAndCount } = props;
    const entityRegistry = useEntityRegistry();
    const linkProps = useEmbeddedProfileLinkProps();

    if (readOnly) {
        return (
            <HoverEntityTooltip entity={term.term}>
                <TermWrapper $showOneAndCount={showOneAndCount}>
                    <TermContent {...props} />
                </TermWrapper>
            </HoverEntityTooltip>
        );
    }

    return (
        <HoverEntityTooltip entity={term.term} width={250}>
            <TermLink
                to={entityRegistry.getEntityUrl(EntityType.GlossaryTerm, term.term.urn)}
                key={term.term.urn}
                $showOneAndCount={showOneAndCount}
                {...linkProps}
            >
                <TermContent {...props} />
            </TermLink>
        </HoverEntityTooltip>
    );
}
