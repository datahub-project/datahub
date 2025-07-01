import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { IconStyleType, PreviewType } from '@app/entityV2/Entity';
import { getSubTypeIcon } from '@app/entityV2/shared/components/subtypes';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import BrowsePaths from '@app/previewV2/BrowsePaths';
import { isDefaultBrowsePath } from '@app/previewV2/utils';
import ParentEntities from '@app/searchV2/filters/ParentEntities';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { BrowsePathEntry, BrowsePathV2, Entity, EntityType } from '@types';

const PlatformContentWrapper = styled.div`
    display: flex;
    flex-wrap: nowrap;
    flex: 1;
    align-items: center;
    max-width: 100%;
    line-height: 22px;
    overflow: hidden;
`;

export const PlatformText = styled.div<{
    $maxWidth?: number;
    $previewType?: Maybe<PreviewType>;
    $isCompactView?: boolean;
    $color?: string;
}>`
    color: ${(props) => props.$color ?? REDESIGN_COLORS.TEXT_GREY};
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
    color: ${ANTD_GRAY[6]};
`;

export function getParentBrowsePathNames(browsePaths?: Maybe<BrowsePathEntry>[] | null) {
    let parentNames = '';
    if (browsePaths) {
        [...browsePaths].reverse().forEach((path, index) => {
            if (path?.name) {
                if (index !== 0) {
                    parentNames += ' > ';
                }
                parentNames += path.name;
            }
        });
    }
    return parentNames;
}

const TypeIconWrapper = styled.span`
    margin-right: 4px;
    display: flex;
    svg {
        font-size: 16px;
    }
`;

const PlatFormTitle = styled.span<{ $color?: string }>`
    display: inline-block;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    color: ${(props) => props.$color ?? REDESIGN_COLORS.TEXT_GREY};
`;

interface Props {
    // eslint-disable-next-line react/no-unused-prop-types
    entityLogoComponent?: JSX.Element;
    // eslint-disable-next-line react/no-unused-prop-types
    instanceId?: string;
    // eslint-disable-next-line react/no-unused-prop-types
    typeIcon?: JSX.Element;
    type?: string;
    entityType: EntityType;
    parentEntities?: Entity[] | null;
    entityTitleWidth?: number;
    previewType?: Maybe<PreviewType>;
    isCompactView?: boolean;
    browsePaths?: Maybe<BrowsePathV2> | undefined;
    contentRef: React.RefObject<HTMLDivElement>;
    isContentTruncated?: boolean;
    linksDisabled?: boolean;
    showPlatformText?: boolean;
    color?: string;
}

function ContextPath(props: Props) {
    const {
        type,
        entityType,
        parentEntities,
        browsePaths,
        entityTitleWidth = 200,
        previewType,
        isCompactView,
        contentRef,
        isContentTruncated = false,
        linksDisabled,
        showPlatformText = true,
        color,
    } = props;

    const entityRegistry = useEntityRegistryV2();
    const entityTypeIcon =
        getSubTypeIcon(type) || entityRegistry.getIcon(entityType, 16, IconStyleType.ACCENT, '#8d95b1');

    const divider = <PlatformDivider>|</PlatformDivider>;

    const hasBrowsePath = !!browsePaths?.path?.length && !isDefaultBrowsePath(browsePaths);
    const hasParentEntities = !!parentEntities?.length;

    const showEntityTypeDivider = hasBrowsePath || hasParentEntities;

    return (
        <PlatformContentWrapper>
            {showPlatformText && (
                <PlatformText
                    $color={color}
                    $maxWidth={entityTitleWidth}
                    $isCompactView={isCompactView}
                    title={capitalizeFirstLetterOnly(type)}
                >
                    {entityTypeIcon && <TypeIconWrapper>{entityTypeIcon}</TypeIconWrapper>}
                    <PlatFormTitle $color={color}>{capitalizeFirstLetterOnly(type)}</PlatFormTitle>
                    {showEntityTypeDivider && divider}
                </PlatformText>
            )}
            {hasBrowsePath ? (
                <BrowsePaths
                    browsePaths={browsePaths}
                    previewType={previewType}
                    contentRef={contentRef}
                    isContentTruncated={isContentTruncated}
                    linksDisabled={linksDisabled}
                    color={color}
                />
            ) : (
                <ParentEntities
                    parentEntities={parentEntities || []}
                    numVisible={3}
                    linksDisabled={linksDisabled}
                    color={color}
                />
            )}
        </PlatformContentWrapper>
    );
}

export default ContextPath;
