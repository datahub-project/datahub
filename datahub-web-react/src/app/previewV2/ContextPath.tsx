import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import styled from 'styled-components';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { IconStyleType } from '@app/entityV2/Entity';
import { getSubTypeIcon } from '@app/entityV2/shared/components/subtypes';
import { getParentEntities } from '@app/entityV2/shared/containers/profile/header/getParentEntities';
import { getDisplayedEntityType } from '@app/entityV2/shared/containers/profile/header/utils';
import BrowsePaths from '@app/previewV2/BrowsePaths';
import { isDefaultBrowsePath } from '@app/previewV2/utils';
import ParentEntities from '@app/searchV2/filters/ParentEntities';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { BrowsePathV2, Entity, EntityType } from '@types';

const PlatformContentWrapper = styled.div`
    display: flex;
    flex-wrap: nowrap;
    align-items: center;
    max-width: 100%;
    line-height: 22px;
    overflow: hidden;
    color: ${(props) => props.theme.colors.text};
`;

export const PlatformText = styled.div<{
    $maxWidth?: number;
    $isCompactView?: boolean;
}>`
    white-space: nowrap;
    text-overflow: ellipsis;
    overflow: hidden;
    display: flex;
    align-items: center;
    ${(props) => props.$maxWidth && `max-width: ${props.$maxWidth}px;`}
    flex-shrink: 0;
`;

const PlatformDivider = styled.hr`
    color: ${(props) => props.theme.colors.border};
    align-self: stretch;
    height: auto;
    margin: 4px;
    border: 0.5px solid;
    vertical-align: text-top;
`;

const TypeIconWrapper = styled.span`
    margin-right: 4px;
    display: flex;
`;

const PlatFormTitle = styled.span`
    display: inline-block;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    color: ${(props) => props.theme.colors.text};
`;

interface Props {
    entityType: EntityType;
    displayedEntityType?: string;
    parentEntities?: Entity[] | null;
    entityTitleWidth?: number;
    isCompactView?: boolean;
    browsePaths?: Maybe<BrowsePathV2> | undefined;
    hideTypeIcons?: boolean;
    linksDisabled?: boolean;
    showPlatformText?: boolean;
    numVisible?: number;
    className?: string;
}

export default function ContextPath(props: Props) {
    const {
        entityType,
        displayedEntityType,
        parentEntities,
        browsePaths,
        entityTitleWidth = 200,
        isCompactView,
        hideTypeIcons,
        linksDisabled,
        showPlatformText = true,
        numVisible,
        className,
    } = props;

    const entityRegistry = useEntityRegistryV2();
    const ICON_SIZE = 14;
    const entityTypeIcon =
        getSubTypeIcon(displayedEntityType, ICON_SIZE) ||
        entityRegistry.getIcon(entityType, ICON_SIZE, IconStyleType.ACCENT);

    const hasBrowsePath = !!browsePaths?.path?.length && !isDefaultBrowsePath(browsePaths);
    const hasParentEntities = !!parentEntities?.length;
    const showEntityTypeDivider = hasBrowsePath || hasParentEntities;

    if (!showPlatformText && !hasBrowsePath && !hasParentEntities) {
        return null;
    }

    return (
        <PlatformContentWrapper className={className}>
            {showPlatformText && (
                <PlatformText
                    $maxWidth={entityTitleWidth}
                    $isCompactView={isCompactView}
                    title={capitalizeFirstLetterOnly(displayedEntityType)}
                >
                    {!hideTypeIcons && entityTypeIcon && <TypeIconWrapper>{entityTypeIcon}</TypeIconWrapper>}
                    <PlatFormTitle>{capitalizeFirstLetterOnly(displayedEntityType)}</PlatFormTitle>
                    {showEntityTypeDivider && <PlatformDivider />}
                </PlatformText>
            )}
            {hasBrowsePath ? (
                <BrowsePaths
                    entries={browsePaths?.path || []}
                    numVisible={numVisible}
                    hideIcons={hideTypeIcons}
                    linksDisabled={linksDisabled}
                />
            ) : (
                <ParentEntities
                    parentEntities={parentEntities || []}
                    numVisible={numVisible}
                    hideIcons={hideTypeIcons}
                    linksDisabled={linksDisabled}
                />
            )}
        </PlatformContentWrapper>
    );
}

type ContextPathProps = Omit<
    Props,
    'parentEntities' | 'browsePaths' | 'entityType' | 'displayedEntityType' | 'contentRef' | 'isContentTruncated'
>;
type GenericEntityProps = Pick<
    GenericEntityProperties,
    'type' | 'browsePathV2' | 'parent' | 'parentContainers' | 'parentDomains' | 'parentNodes'
>;
type GenericPropertiesContextPathProps = ContextPathProps & { properties: GenericEntityProps };

export function GenericPropertiesContextPath({ properties, ...props }: GenericPropertiesContextPathProps) {
    const entityRegistry = useEntityRegistryV2();

    if (!properties.type) {
        return null;
    }
    const displayedEntityType = getDisplayedEntityType(properties, entityRegistry, properties.type);
    const parentEntities = getParentEntities(properties);

    return (
        <ContextPath
            {...props}
            displayedEntityType={displayedEntityType}
            entityType={properties.type}
            browsePaths={properties?.browsePathV2}
            parentEntities={parentEntities}
        />
    );
}
