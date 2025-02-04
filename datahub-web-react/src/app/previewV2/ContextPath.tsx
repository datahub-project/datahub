import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import styled from 'styled-components';
import { BrowsePathEntry, BrowsePathV2, Entity, EntityType } from '../../types.generated';
import { getSubTypeIcon } from '../entityV2/shared/components/subtypes';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';
import ParentEntities from '../searchV2/filters/ParentEntities';
import { capitalizeFirstLetterOnly } from '../shared/textUtil';
import { useEntityRegistryV2 } from '../useEntityRegistry';
import { IconStyleType, PreviewType } from '../entityV2/Entity';
import BrowsePaths from './BrowsePaths';
import { ANTD_GRAY } from '../entity/shared/constants';
import { isDefaultBrowsePath } from './utils';

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

const PlatFormTitle = styled.span`
    display: inline-block;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    color: ${REDESIGN_COLORS.TEXT_GREY};
`;

interface Props {
    // eslint-disable-next-line react/no-unused-prop-types
    entityLogoComponent?: JSX.Element;
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
}

function ContextPath(props: Props) {
    const {
        type,
        entityType,
        parentEntities,
        browsePaths,
        instanceId,
        entityTitleWidth = 200,
        previewType,
        isCompactView,
        contentRef,
        isContentTruncated = false,
        linksDisabled,
    } = props;

    const entityRegistry = useEntityRegistryV2();
    const entityTypeIcon =
        getSubTypeIcon(type) || entityRegistry.getIcon(entityType, 16, IconStyleType.ACCENT, '#8d95b1');

    const divider = <PlatformDivider>|</PlatformDivider>;

    const hasPlatformInstance = !!instanceId;
    const hasBrowsePath = !!browsePaths?.path?.length && !isDefaultBrowsePath(browsePaths);
    const hasParentEntities = !!parentEntities?.length;

    const showInstanceIdDivider = hasBrowsePath || hasParentEntities;
    const showEntityTypeDivider = hasPlatformInstance || hasBrowsePath || hasParentEntities;

    return (
        <PlatformContentWrapper>
            <PlatformText
                $maxWidth={entityTitleWidth}
                $isCompactView={isCompactView}
                title={capitalizeFirstLetterOnly(type)}
            >
                {entityTypeIcon && <TypeIconWrapper>{entityTypeIcon}</TypeIconWrapper>}
                <PlatFormTitle>{capitalizeFirstLetterOnly(type)}</PlatFormTitle>
                {showEntityTypeDivider && divider}
            </PlatformText>
            {instanceId && (
                <PlatformText>
                    {instanceId}
                    {showInstanceIdDivider && divider}
                </PlatformText>
            )}
            {hasBrowsePath ? (
                <BrowsePaths
                    browsePaths={browsePaths}
                    previewType={previewType}
                    contentRef={contentRef}
                    isContentTruncated={isContentTruncated}
                    linksDisabled={linksDisabled}
                />
            ) : (
                <ParentEntities parentEntities={parentEntities || []} numVisible={3} linksDisabled={linksDisabled} />
            )}
        </PlatformContentWrapper>
    );
}

export default ContextPath;
