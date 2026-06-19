import { Popover } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import EntityRegistry from '@app/entityV2/EntityRegistry';
import { isNullOrUndefined } from '@app/previewV2/utils';
import { useEmbeddedProfileLinkProps } from '@app/shared/useEmbeddedProfileLinkProps';

import { EntityType } from '@types';

import LineageStatusIcon from '@images/lineage-status.svg?react';

const Icon = styled(LineageStatusIcon)<{ highlighted?: boolean }>`
    display: flex;
    color: ${({ highlighted, theme }) => (highlighted ? theme.colors.iconBrand : theme.colors.iconDisabled)};
    font-size: 16px;

    :hover {
        ${({ highlighted, theme }) => highlighted && `color: ${theme.colors.iconSelected};`}
    }
`;

const PopoverContent = styled.div`
    align-items: center;
    display: flex;
    color: ${(props) => props.theme.colors.textSecondary};
`;

interface Props {
    upstreamTotal: number | null | undefined;
    downstreamTotal: number | null | undefined;
    entityRegistry: EntityRegistry;
    entityType: EntityType;
    urn: string;
}

export default function LineageBadge({ upstreamTotal, downstreamTotal, entityRegistry, entityType, urn }: Props) {
    const { t } = useTranslation('entity.preview');
    const hasLineage = !!upstreamTotal || !!downstreamTotal;
    const linkProps = useEmbeddedProfileLinkProps();

    const upstreamContent = upstreamTotal ? t('lineage.upstreamCount', { count: upstreamTotal }) : '';
    const downstreamContent = downstreamTotal ? t('lineage.downstreamCount', { count: downstreamTotal }) : '';
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
                        {/* eslint-disable i18next/no-literal-string -- (untranslated-text) punctuation separator */}
                        {separator}
                        {/* eslint-enable i18next/no-literal-string */}
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
