import React from 'react';
import styled from 'styled-components';
import { Typography, Image } from 'antd';
import { Maybe } from 'graphql/jsutils/Maybe';
import { Container, Dataset, Entity } from '../../../../../../../types.generated';
import { ANTD_GRAY } from '../../../../constants';
import ContainerLink from './ContainerLink';
import DatasetLink from './DatasetLink';
import {
    StyledRightOutlined,
    ParentNodesWrapper as ParentContainersWrapper,
    Ellipsis,
    StyledTooltip,
} from './ParentNodesView';
import ParentEntities from '../../../../../../search/filters/ParentEntities';
import { useIsShowSeparateSiblingsEnabled } from '../../../../../../useAppConfig';

export const LogoIcon = styled.span`
    display: flex;
    gap: 4px;
    margin-right: 8px;
`;

export const PreviewImage = styled(Image)`
    max-height: 17px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
`;

export const PlatformContentWrapper = styled.div`
    display: flex;
    align-items: center;
    margin: 0 8px 6px 0;
    flex-wrap: nowrap;
    flex: 1;
`;

export const PlatformText = styled(Typography.Text)`
    font-size: 12px;
    line-height: 20px;
    font-weight: 700;
    color: ${ANTD_GRAY[7]};
    white-space: nowrap;
`;

const PlatformDivider = styled.div`
    display: inline-block;
    padding-left: 8px;
    margin-right: 8px;
    border-right: 1px solid ${ANTD_GRAY[4]};
    height: 18px;
    vertical-align: text-top;
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

interface Props {
    platformName?: string;
    platformLogoUrl?: Maybe<string>;
    platformNames?: Maybe<string>[];
    platformLogoUrls?: Maybe<string>[];
    entityLogoComponent?: JSX.Element;
    instanceId?: string;
    typeIcon?: JSX.Element;
    entityType?: string;
    parentContainers?: Maybe<Container>[] | null;
    parentEntities?: Entity[] | null;
    parentContainersRef: React.RefObject<HTMLDivElement>;
    areContainersTruncated: boolean;
    parentDataset?: Dataset;
}

function PlatformContentView(props: Props) {
    const {
        parentEntities,
        platformName,
        platformLogoUrl,
        platformNames,
        platformLogoUrls,
        entityLogoComponent,
        instanceId,
        typeIcon,
        entityType,
        parentContainers,
        parentContainersRef,
        areContainersTruncated,
        parentDataset,
    } = props;

    const directParentContainer = parentContainers && parentContainers[0];
    const remainingParentContainers = parentContainers && parentContainers.slice(1, parentContainers.length);

    const shouldShowSeparateSiblings = useIsShowSeparateSiblingsEnabled();
    const showSiblingPlatformLogos = !shouldShowSeparateSiblings && !!platformLogoUrls;
    const showSiblingPlatformNames = !shouldShowSeparateSiblings && !!platformNames;

    return (
        <PlatformContentWrapper>
            {typeIcon && <LogoIcon>{typeIcon}</LogoIcon>}
            <PlatformText>{entityType}</PlatformText>
            {(!!platformName || !!instanceId || !!parentContainers?.length || !!parentEntities?.length) && (
                <PlatformDivider />
            )}
            {platformName && (
                <LogoIcon>
                    {!platformLogoUrl && !platformLogoUrls && entityLogoComponent}
                    {!!platformLogoUrl && !showSiblingPlatformLogos && (
                        <PreviewImage preview={false} src={platformLogoUrl} alt={platformName} />
                    )}
                    {showSiblingPlatformLogos &&
                        platformLogoUrls.slice(0, 2).map((platformLogoUrlsEntry) => (
                            <>
                                <PreviewImage preview={false} src={platformLogoUrlsEntry || ''} alt={platformName} />
                            </>
                        ))}
                </LogoIcon>
            )}
            <PlatformText>
                {showSiblingPlatformNames ? platformNames.join(' & ') : platformName}
                {(directParentContainer || instanceId) && <StyledRightOutlined data-testid="right-arrow" />}
            </PlatformText>
            {instanceId && (
                <PlatformText>
                    {instanceId}
                    {directParentContainer && <StyledRightOutlined data-testid="right-arrow" />}
                </PlatformText>
            )}
            <StyledTooltip
                title={getParentContainerNames(parentContainers)}
                overlayStyle={areContainersTruncated ? {} : { display: 'none' }}
            >
                {areContainersTruncated && <Ellipsis>...</Ellipsis>}
                <ParentContainersWrapper ref={parentContainersRef}>
                    {remainingParentContainers &&
                        remainingParentContainers.map((container) => (
                            <span key={container?.urn}>
                                <ContainerLink container={container} />
                                <StyledRightOutlined data-testid="right-arrow" />
                            </span>
                        ))}
                </ParentContainersWrapper>
                {directParentContainer && <ContainerLink container={directParentContainer} />}
            </StyledTooltip>
            {parentDataset && (
                <span>
                    <StyledRightOutlined data-testid="right-arrow" />
                    <DatasetLink parentDataset={parentDataset} />
                </span>
            )}
            <ParentEntities parentEntities={parentEntities || []} numVisible={3} />
        </PlatformContentWrapper>
    );
}

export default PlatformContentView;
