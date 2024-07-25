import React from 'react';
import styled from 'styled-components';
import KeyboardArrowRightOutlinedIcon from '@mui/icons-material/KeyboardArrowRightOutlined';

import { REDESIGN_COLORS } from '../entityV2/shared/constants';
import { getSubTypeIcon } from '../entityV2/shared/components/subtypes';
import { BrowsePathV2, Dataset, EntityType, Maybe } from '../../types.generated';
import { capitalizeFirstLetterOnly } from '../shared/textUtil';
import { useEntityRegistryV2 } from '../useEntityRegistry';
import { IconStyleType } from '../entityV2/Entity';
import ContainerLink from './SearchCardBrowsePathContainerLink';
import { GenericEntityProperties } from '../entity/shared/types';

const PlatformContentWrapper = styled.div`
    display: flex;
    margin-right: 8px;
    flex-wrap: nowrap;
    flex: 1;
    align-items: center;
    max-width: 100%;
    line-height: 28px;
`;

export const PlatformText = styled.div<{
    $maxWidth?: number;
    $isCompactView?: boolean;
}>`
    color: ${REDESIGN_COLORS.TEXT_GREY};
    white-space: nowrap;
    font-family: Mulish;
    font-style: normal;
    font-weight: 500;
    text-overflow: ellipsis;
    overflow: hidden;
    display: flex;
    align-items: center;
    ${(props) => (props.$isCompactView ? '12px' : '13px')}
    ${(props) => props.$maxWidth && `max-width: ${props.$maxWidth}px;`}
`;

const PlatformDivider = styled.div`
    padding-left: 6px;
    margin-right: 6px;
    font-size: 16px;
    margin-top: -3px;
    color: ${REDESIGN_COLORS.TEXT_GREY};
`;

const PlatFormTitle = styled.span`
    display: inline-block;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
`;

const TypeIconWrapper = styled.span`
    margin-right: 4px;
    display: flex;
    svg {
        font-size: 16px;
    }
`;

interface Props {
    entityType: EntityType;
    type?: string;
    isCompactView?: boolean;
    browsePaths?: Maybe<BrowsePathV2> | undefined;
    parentEntity?: Maybe<GenericEntityProperties> | undefined;
}

function StaticSearchCardBrowsePath({ browsePaths, entityType, type, isCompactView, parentEntity }: Props) {
    const entityRegistry = useEntityRegistryV2();
    const paths = browsePaths?.path || [];
    const entityTypeIcon =
        getSubTypeIcon(type) || entityRegistry.getIcon(entityType, 16, IconStyleType.ACCENT, '#8d95b1');

    const divider = isCompactView ? (
        <KeyboardArrowRightOutlinedIcon style={{ fill: REDESIGN_COLORS.TEXT_GREY, fontSize: 20, marginBottom: -1 }} />
    ) : (
        <PlatformDivider>|</PlatformDivider>
    );

    return (
        <PlatformContentWrapper>
            {/* This always renders */}
            <PlatformText $maxWidth={150} $isCompactView={isCompactView} title={capitalizeFirstLetterOnly(type)}>
                {entityTypeIcon && <TypeIconWrapper>{entityTypeIcon}</TypeIconWrapper>}
                <PlatFormTitle>{capitalizeFirstLetterOnly(type)}</PlatFormTitle>
                {(paths.length > 0 || parentEntity) && divider}
            </PlatformText>
            {/* For parentEntity type */}
            {parentEntity && (
                <>
                    <ContainerLink key={parentEntity.urn} container={parentEntity as Dataset} />
                    {paths.length > 0 && divider}
                </>
            )}
            {/* Render the paths if they exist */}
            {browsePaths &&
                paths.map((path: any, key: number) => {
                    const hasEntity = path.entity !== null;
                    const pathEntityType = path.entity?.type || path.type;

                    let renderComponent: JSX.Element | null = null;

                    /* If the path has an entity, render it's link */

                    switch (hasEntity && pathEntityType) {
                        case EntityType.Container:
                        case EntityType.Dashboard:
                        case EntityType.Dataset:
                            renderComponent = <ContainerLink key={path.urn} container={path.entity} />;
                            break;
                        default:
                            renderComponent = (
                                <PlatformText $maxWidth={150}>
                                    <PlatFormTitle>{path.name}</PlatFormTitle>
                                </PlatformText>
                            );
                            break;
                    }

                    return (
                        <>
                            {renderComponent}
                            {key !== paths.length - 1 && divider}
                        </>
                    );
                })}
        </PlatformContentWrapper>
    );
}

export default StaticSearchCardBrowsePath;
