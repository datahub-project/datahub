import React from 'react';
import styled from 'styled-components';
import { Link } from 'react-router-dom';
import { Popover } from '@components';
import { EntityType } from '../../types.generated';
import EntityRegistry from '../entityV2/EntityRegistry';
import LineageStatusIcon from '../../images/lineage-status.svg?react';
import { ANTD_GRAY, REDESIGN_COLORS, SEARCH_COLORS } from '../entityV2/shared/constants';
import { pluralize } from '../shared/textUtil';
import { isNullOrUndefined } from './utils';
import { useEmbeddedProfileLinkProps } from '../shared/useEmbeddedProfileLinkProps';

const Icon = styled(LineageStatusIcon)<{ highlighted?: boolean }>`
    display: flex;
    color: ${({ highlighted }) => (highlighted ? SEARCH_COLORS.SUBTEXT_PURPPLE : ANTD_GRAY[5.5])};
    font-size: 16px;

    :hover {
        ${({ highlighted }) => highlighted && `color: ${SEARCH_COLORS.LINK_BLUE};`}
`;

const PopoverContent = styled.div`
    align-items: center;
    display: flex;
    color: ${REDESIGN_COLORS.DARK_GREY};
`;

interface Props {
    upstreamTotal: number | null | undefined;
    downstreamTotal: number | null | undefined;
    entityRegistry: EntityRegistry;
    entityType: EntityType;
    urn: string;
}

export default function LineageBadge({ upstreamTotal, downstreamTotal, entityRegistry, entityType, urn }: Props) {
    const hasLineage = !!upstreamTotal || !!downstreamTotal;
    const linkProps = useEmbeddedProfileLinkProps();

    const upstreamContent = upstreamTotal ? `${upstreamTotal} ${pluralize(upstreamTotal, 'upstream')}` : '';
    const downstreamContent = downstreamTotal ? `${downstreamTotal} ${pluralize(downstreamTotal, 'downstream')}` : '';
    const separator = upstreamContent && downstreamContent ? ', ' : '';

    if (isNullOrUndefined(upstreamTotal) && isNullOrUndefined(downstreamTotal)) {
        return null;
    }

    return (
        <Popover
            content={
                hasLineage && (
                    <PopoverContent>
                        {upstreamContent}
                        {separator}
                        {downstreamContent}
                    </PopoverContent>
                )
            }
            showArrow={false}
            placement="bottom"
        >
            {hasLineage && (
                <Link to={`${entityRegistry.getEntityUrl(entityType, urn)}/Lineage`} {...linkProps}>
                    <Icon highlighted />
                </Link>
            )}
            {!hasLineage && <Icon highlighted={false} />}
        </Popover>
    );
}
