import React from 'react';
import styled from 'styled-components';
import { Typography, Image, Tooltip } from 'antd';
import { FolderOutlined, RightOutlined } from '@ant-design/icons';
import { Maybe } from 'graphql/jsutils/Maybe';
import { Container, GlossaryNode } from '../../../../../../../types.generated';
import { ANTD_GRAY } from '../../../../constants';
import ContainerLink from './ContainerLink';
import { capitalizeFirstLetterOnly } from '../../../../../../shared/textUtil';

const LogoIcon = styled.span`
    display: flex;
    margin-right: 8px;
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
    margin: 0 8px 6px 0;
    flex-wrap: nowrap;
    flex: 1;
`;

const PlatformText = styled(Typography.Text)`
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

const StyledRightOutlined = styled(RightOutlined)`
    color: ${ANTD_GRAY[7]};
    font-size: 8px;
    margin: 0 10px;
`;

const ParentContainersWrapper = styled.div`
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    flex-direction: row-reverse;
    display: flex;
`;

const Ellipsis = styled.span`
    color: ${ANTD_GRAY[7]};
    margin-right: 2px;
`;

const StyledTooltip = styled(Tooltip)`
    display: flex;
    white-space: nowrap;
    overflow: hidden;
`;

const GlossaryNodeText = styled(Typography.Text)`
    font-size: 12px;
    line-height: 20px;
    color: ${ANTD_GRAY[7]};
`;

const GlossaryNodeIcon = styled(FolderOutlined)`
    color: ${ANTD_GRAY[7]};

    &&& {
        font-size: 12px;
        margin-right: 4px;
    }
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
    parentNodes?: GlossaryNode[] | null;
    parentContainersRef: React.RefObject<HTMLDivElement>;
    areContainersTruncated: boolean;
}

function PlatformContentView(props: Props) {
    const {
        parentNodes,
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

    return (
        <PlatformContentWrapper>
            {typeIcon && <LogoIcon>{typeIcon}</LogoIcon>}
            <PlatformText>{capitalizeFirstLetterOnly(entityType)}</PlatformText>
            {(!!platformName || !!instanceId || !!parentContainers?.length || !!parentNodes?.length) && (
                <PlatformDivider />
            )}
            {platformName && (
                <LogoIcon>
                    {!platformLogoUrl && !platformLogoUrls && entityLogoComponent}
                    {!!platformLogoUrl && !platformLogoUrls && (
                        <PreviewImage preview={false} src={platformLogoUrl} alt={platformName} />
                    )}
                    {!!platformLogoUrls &&
                        platformLogoUrls.slice(0, 2).map((platformLogoUrlsEntry) => (
                            <>
                                <PreviewImage preview={false} src={platformLogoUrlsEntry || ''} alt={platformName} />
                            </>
                        ))}
                </LogoIcon>
            )}
            <PlatformText>
                {platformNames ? platformNames.join(' & ') : platformName}
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
            {[...(parentNodes || [])]?.reverse()?.map((parentNode, idx) => (
                <>
                    <GlossaryNodeIcon />
                    <GlossaryNodeText>{parentNode?.properties?.name}</GlossaryNodeText>
                    {idx + 1 !== parentNodes?.length && <StyledRightOutlined data-testid="right-arrow" />}
                </>
            ))}
        </PlatformContentWrapper>
    );
}

export default PlatformContentView;
