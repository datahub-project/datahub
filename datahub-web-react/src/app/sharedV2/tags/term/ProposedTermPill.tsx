import { colors } from '@src/alchemy-components';
import ProposedIcon from '@src/app/entityV2/shared/sidebarSection/ProposedIcon';
import { EntityType, GlossaryTerm } from '@src/types.generated';
import React from 'react';
import styled from 'styled-components';
import { Tag } from 'antd';
import { ANTD_GRAY, REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { generateColorFromPalette } from '@src/app/glossaryV2/colorUtils';
import { TermRibbon } from './TermContent';

const TermContainer = styled.div<{ $isApproved?: boolean }>`
    display: flex;
    max-width: inherit;

    .ant-tag.ant-tag {
        border-radius: 5px;
        border: 1px ${(props) => (props.$isApproved ? 'solid' : 'dashed')} ${colors.gray[200]};
    }
`;

export const Term = styled(Tag)`
    margin: 0;
    padding: 3px 8px;
    font-size: 12px;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
    position: relative;
    overflow: hidden;
    border: 1px dashed ${colors.gray[200]};
    display: flex;
    align-items: center;
`;

const ProposedTermText = styled.span`
    margin-left: 8px;
    text-overflow: ellipsis;
    overflow: hidden;
`;

interface Props {
    term: GlossaryTerm;
    onClick?: (e) => void;
    isApproved?: boolean;
    showClockIcon?: boolean;
}

const ProposedTermPill = ({ term, onClick, isApproved, showClockIcon = true }: Props) => {
    const entityRegistry = useEntityRegistryV2();

    const urn = term?.urn;
    const parentNodes = term?.parentNodes;
    const lastParentNode = parentNodes && parentNodes.count > 0 && parentNodes.nodes[parentNodes.count - 1];
    const termColor = lastParentNode
        ? lastParentNode.displayProperties?.colorHex || generateColorFromPalette(lastParentNode.urn)
        : (urn && generateColorFromPalette(urn)) || ANTD_GRAY[6];
    const termName = entityRegistry.getDisplayName(EntityType.GlossaryTerm, term);

    return (
        <TermContainer $isApproved={isApproved}>
            <Term closable={false} data-testid={`proposed-term-${term?.name}`} onClick={onClick}>
                <TermRibbon opacity={0.5} color={termColor} />
                <ProposedTermText>{termName}</ProposedTermText>
                {showClockIcon && <ProposedIcon propertyName="Term" />}
            </Term>
        </TermContainer>
    );
};

export default ProposedTermPill;
