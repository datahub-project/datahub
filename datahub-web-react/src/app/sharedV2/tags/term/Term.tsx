import React, { useState } from 'react';
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
    const previewContext = { propagationDetails: { context: props.context, attribution: term.attribution } };
    const [isDeleteModalOpen, setIsDeleteModalOpen] = useState(false);

    if (readOnly) {
        return (
            <HoverEntityTooltip entity={term.term} previewContext={previewContext}>
                <TermWrapper $showOneAndCount={showOneAndCount}>
                    <TermContent {...props} />
                </TermWrapper>
            </HoverEntityTooltip>
        );
    }

    return (
        <HoverEntityTooltip canOpen={!isDeleteModalOpen} entity={term.term} width={250} previewContext={previewContext}>
            <TermLink
                to={entityRegistry.getEntityUrl(EntityType.GlossaryTerm, term.term.urn)}
                key={term.term.urn}
                $showOneAndCount={showOneAndCount}
                {...linkProps}
            >
                <TermContent
                    {...props}
                    onOpenModal={() => setIsDeleteModalOpen(true)}
                    onCloseModal={() => setIsDeleteModalOpen(false)}
                />
            </TermLink>
        </HoverEntityTooltip>
    );
}
