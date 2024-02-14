import { Typography } from 'antd';
import { Maybe } from 'graphql/jsutils/Maybe';
import React from 'react';
import styled from 'styled-components';
import { Container, Entity, EntityType } from '../../types.generated';
import { getSubTypeIcon } from '../entityV2/shared/components/subtypes';
import { SEARCH_COLORS } from '../entityV2/shared/constants';
import {
    Ellipsis,
    ParentNodesWrapper,
    StyledTooltip,
} from '../entityV2/shared/containers/profile/header/PlatformContent/ParentNodesView';
import ParentEntities from '../searchV2/filters/ParentEntities';
import { capitalizeFirstLetter } from '../shared/textUtil';
import { useEntityRegistry } from '../useEntityRegistry';
import ContainerLink from './SearchCardBrowsePathContainerLink';
import { PreviewType } from '../entityV2/Entity';

const PlatformContentWrapper = styled.div`
    display: flex;
    margin: 0px 8px 6px 0;
    flex-wrap: nowrap;
    flex: 1;
    align-items: baseline;
    max-width: 100%;
`;

const PlatformText = styled(Typography.Text)<{
    $maxWidth?: number;
    $previewType?: Maybe<PreviewType>;
}>`
    color: ${SEARCH_COLORS.PLATFORM_TEXT};
    white-space: nowrap;
    font-family: Mulish;
    font-size: 13px;
    font-style: normal;
    font-weight: 500;
    line-height: normal;
    text-overflow: ellipsis;
    overflow: hidden;
    max-width: ${(props) => props.$maxWidth}px;
`;

const PlatformDivider = styled.div`
    display: inline-block;
    padding-left: 6px;
    margin-right: 6px;
    height: 16px;
    margin-bottom: -2px;
    font-size: 16px;
    position: relative;
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
    entityTitleWidth: number;
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
    const entityRegistry = useEntityRegistry();

    const directParentContainer = parentContainers && parentContainers[0];
    const remainingParentContainers = parentContainers && parentContainers.slice(1, parentContainers.length);
    const entityTypeIcon = getSubTypeIcon(type) || entityRegistry.getIcon(entityType, 11);
    return (
        <PlatformContentWrapper>
            <PlatformText
                $maxWidth={entityTitleWidth}
                title={capitalizeFirstLetter(type)}
            >
                {entityTypeIcon && <TypeIconWrapper>{entityTypeIcon}</TypeIconWrapper>}
                {capitalizeFirstLetter(type)}
            </PlatformText>
            {(!!instanceId || !!parentContainers?.length || !!parentEntities?.length) && (
                <PlatformDivider>/</PlatformDivider>
            )}
            {instanceId && (
                <PlatformText>
                    {instanceId}
                    {directParentContainer && <PlatformDivider>/</PlatformDivider>}
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
                                    <PlatformDivider>/</PlatformDivider>
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
