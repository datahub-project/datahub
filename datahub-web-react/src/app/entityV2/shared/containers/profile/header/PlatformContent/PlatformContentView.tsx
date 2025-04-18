import React from 'react';
import styled from 'styled-components';
import { Typography, Image } from 'antd';
import { useIsShowSeparateSiblingsEnabled } from '@src/app/useAppConfig';
import { Maybe } from 'graphql/jsutils/Maybe';
import { Container, Entity } from '../../../../../../../types.generated';
import ContainerLink from './ContainerLink';
import { ParentNodesWrapper as ParentContainersWrapper, Ellipsis, StyledTooltip } from './ParentNodesView';
import ParentEntities from '../../../../../../search/filters/ParentEntities';

const LogoIcon = styled.span`
    display: flex;
    gap: 4px;
    margin-right: 4px;
`;

const PreviewImage = styled(Image)`
    max-height: 17px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
`;

const PlatformContentWrapper = styled.div`
    display: flex;
    align-items: center;
    margin: 0px 8px 0px 0;
    flex-wrap: nowrap;
    flex: 1;
`;

const PlatformText = styled(Typography.Text)`
    font-size: 10px;
    font-weight: 400;
    color: #6c6b88;
    text-transform: capitalize;
    white-space: nowrap;
`;

const PlatformDivider = styled.div`
    display: inline-block;
    margin: 0 10px;
    border-left: 1px solid #d5d5d5;
    height: 16px;
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
    } = props;

    const directParentContainer = parentContainers && parentContainers[0];
    const remainingParentContainers = parentContainers && parentContainers.slice(1, parentContainers.length);

    const shouldShowSeparateSiblings = useIsShowSeparateSiblingsEnabled();
    const showSiblingPlatformLogos = !shouldShowSeparateSiblings && !!platformLogoUrls;
    const showSiblingPlatformNames = !shouldShowSeparateSiblings && !!platformNames;

    return (
        <PlatformContentWrapper>
            {platformName && (
                <>
                    <LogoIcon>
                        {!platformLogoUrl && !platformLogoUrls && entityLogoComponent}
                        {!!platformLogoUrl && !showSiblingPlatformLogos && (
                            <PreviewImage preview={false} src={platformLogoUrl} alt={platformName} />
                        )}
                        {!!showSiblingPlatformLogos &&
                            platformLogoUrls.slice(0, 2).map((platformLogoUrlsEntry) => (
                                <>
                                    <PreviewImage
                                        preview={false}
                                        src={platformLogoUrlsEntry || ''}
                                        alt={platformName}
                                    />
                                </>
                            ))}
                    </LogoIcon>
                    <PlatformText>
                        <span>{showSiblingPlatformNames ? platformNames.join(' & ') : platformName}</span>
                    </PlatformText>
                    <PlatformDivider data-testid="platform-divider" />
                </>
            )}
            {instanceId && (
                <PlatformText>
                    {instanceId}
                    <PlatformDivider data-testid="divider" />
                </PlatformText>
            )}
            {typeIcon && <LogoIcon>{typeIcon}</LogoIcon>}
            <PlatformText>{entityType}</PlatformText>
            {(!!parentContainers?.length || !!parentEntities?.length) && <PlatformDivider data-testid="divider" />}
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
                                <PlatformDivider data-testid="divider" />
                            </span>
                        ))}
                </ParentContainersWrapper>
                {directParentContainer && <ContainerLink container={directParentContainer} />}
            </StyledTooltip>
            <ParentEntities parentEntities={parentEntities || []} numVisible={3} />
        </PlatformContentWrapper>
    );
}

export default PlatformContentView;
