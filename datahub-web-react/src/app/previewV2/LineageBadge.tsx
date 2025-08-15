import { Popover } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import EntityRegistry from '@app/entityV2/EntityRegistry';
import { ANTD_GRAY, REDESIGN_COLORS, SEARCH_COLORS } from '@app/entityV2/shared/constants';
import { isNullOrUndefined } from '@app/previewV2/utils';
import { pluralize } from '@app/shared/textUtil';
import { useEmbeddedProfileLinkProps } from '@app/shared/useEmbeddedProfileLinkProps';

import { EntityType } from '@types';

import LineageStatusIcon from '@images/lineage-status.svg?react';

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
