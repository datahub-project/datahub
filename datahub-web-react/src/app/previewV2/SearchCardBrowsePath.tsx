import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import styled from 'styled-components';
import { Container, Entity, EntityType } from '../../types.generated';
import { getSubTypeIcon } from '../entityV2/shared/components/subtypes';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';
import {
    Ellipsis,
    ParentNodesWrapper,
    StyledTooltip,
} from '../entityV2/shared/containers/profile/header/PlatformContent/ParentNodesView';
import ParentEntities from '../searchV2/filters/ParentEntities';
import { capitalizeFirstLetter } from '../shared/textUtil';
import { useEntityRegistryV2 } from '../useEntityRegistry';
import ContainerLink from './SearchCardBrowsePathContainerLink';
import { IconStyleType, PreviewType } from '../entityV2/Entity';

const PlatformContentWrapper = styled.div`
    display: flex;
    margin-right: 8px;
    flex-wrap: nowrap;
    flex: 1;
    align-items: center;
    max-width: 100%;
`;

export const PlatformText = styled.div<{
    $maxWidth?: number;
    $previewType?: Maybe<PreviewType>;
}>`
    color: ${REDESIGN_COLORS.TEXT_GREY};
    white-space: nowrap;
    font-family: Mulish;
    font-size: 13px;
    font-style: normal;
    font-weight: 500;
    text-overflow: ellipsis;
    overflow: hidden;
    display: flex;
    align-items: center;
    ${(props) => props.$maxWidth && `max-width: ${props.$maxWidth}px;`}
`;

const PlatformDivider = styled.div`
    padding-left: 6px;
    margin-right: 6px;
    font-size: 16px;
    line-height: 16px;
    color: #edeef2;
`;

export function getParentContainerNames(containers?: Maybe<Container>[] | null) {
    let parentNames = '';
    if (containers) {
        [...containers].reverse().forEach((container, index) => {
            if (container?.properties) {
                if (index !== 0) {
                    parentNames += ' > ';
                }
                parentNames += container.properties.name;
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

interface Props {
    // eslint-disable-next-line react/no-unused-prop-types
    entityLogoComponent?: JSX.Element;
    instanceId?: string;
    // eslint-disable-next-line react/no-unused-prop-types
    typeIcon?: JSX.Element;
    type?: string;
    entityType: EntityType;
    parentContainers?: Maybe<Container>[] | null;
    parentEntities?: Entity[] | null;
    parentContainersRef: React.RefObject<HTMLDivElement>;
    areContainersTruncated: boolean;
    entityTitleWidth?: number;
    previewType?: Maybe<PreviewType>;
}

function SearchCardBrowsePath(props: Props) {
    const {
        parentEntities,
        instanceId,
        type,
        entityType,
        parentContainers,
        parentContainersRef,
        areContainersTruncated,
        entityTitleWidth = 200,
        previewType,
    } = props;
    const entityRegistry = useEntityRegistryV2();

    const directParentContainer = parentContainers && parentContainers[0];
    const remainingParentContainers = parentContainers && parentContainers.slice(1, parentContainers.length);
    const entityTypeIcon =
        getSubTypeIcon(type) || entityRegistry.getIcon(entityType, 16, IconStyleType.ACCENT, '#8d95b1');
    return (
        <PlatformContentWrapper>
            <PlatformText $maxWidth={entityTitleWidth} title={capitalizeFirstLetter(type)}>
                {entityTypeIcon && <TypeIconWrapper>{entityTypeIcon}</TypeIconWrapper>}
                {capitalizeFirstLetter(type)}
                {(!!instanceId || !!parentContainers?.length || !!parentEntities?.length) && (
                    <PlatformDivider>|</PlatformDivider>
                )}
            </PlatformText>

            {instanceId && (
                <PlatformText>
                    {instanceId}
                    {directParentContainer && <PlatformDivider>|</PlatformDivider>}
                </PlatformText>
            )}
            <StyledTooltip
                title={getParentContainerNames(parentContainers)}
                overlayStyle={areContainersTruncated ? {} : { display: 'none' }}
                maxWidth={previewType === PreviewType.HOVER_CARD ? 300 : 620}
            >
                {areContainersTruncated && <Ellipsis>...</Ellipsis>}
                <ParentNodesWrapper ref={parentContainersRef}>
                    {directParentContainer && <ContainerLink container={directParentContainer} />}
                    {remainingParentContainers &&
                        remainingParentContainers.map((container) => (
                            <span key={container?.urn}>
                                <PlatformText>
                                    <ContainerLink container={container} />
                                    <PlatformDivider>|</PlatformDivider>
                                </PlatformText>
                            </span>
                        ))}
                </ParentNodesWrapper>
            </StyledTooltip>
            <ParentEntities parentEntities={parentEntities || []} numVisible={3} />
        </PlatformContentWrapper>
    );
}

export default SearchCardBrowsePath;
