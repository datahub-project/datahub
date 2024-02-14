import { Tooltip } from 'antd';
import { Maybe } from 'graphql/jsutils/Maybe';
import OutputIcon from '@mui/icons-material/Output';
import React from 'react';
import styled from 'styled-components';
import ImageWithColoredBackground, { Icon } from './ImageWIthColoredBackground';
import { ANTD_GRAY } from '../entityV2/shared/constants';

const LogoIcon = styled.span`
    display: flex;
    gap: 4px;
    margin-right: 8px;
`;

const PlatformContentWrapper = styled.div`
    display: flex;
    align-items: center;
    margin: 0 8px 8px 0;
    flex-wrap: nowrap;
    flex: 1;
`;

interface Props {
    platformName?: string;
    platformLogoUrl?: Maybe<string>;
    platformNames?: Maybe<string>[];
    platformLogoUrls?: Maybe<string>[];
    entityLogoComponent?: JSX.Element;
    isOutputPort?: boolean;
    icon?: React.ReactNode;
}

export default function ColoredBackgroundPlatformIconGroup(props: Props) {
    const { platformName, platformLogoUrl, platformNames, platformLogoUrls, entityLogoComponent, isOutputPort, icon } =
        props;

    const renderLogoIcon = () => {
        if (icon) {
            // Render only the provided icon
            return <LogoIcon>{icon}</LogoIcon>;
        }

        // Render other icons
        return (
            <>
                {platformName && (
                    <LogoIcon>
                        {!platformLogoUrl && !platformLogoUrls && entityLogoComponent}
                        {!!platformLogoUrl && !platformLogoUrls && (
                            <ImageWithColoredBackground
                                src={platformLogoUrl}
                                alt={platformName || ''}
                                borderRadius={10}
                                backgroundSize={30}
                                imgSize={20}
                            />
                        )}
                        {!!platformLogoUrls &&
                            platformLogoUrls
                                .slice(0, 2)
                                .map((url, idx) => (
                                    <ImageWithColoredBackground
                                        key={url}
                                        borderRadius={10}
                                        backgroundSize={30}
                                        imgSize={20}
                                        src={url || ''}
                                        alt={platformNames?.[idx] || ''}
                                    />
                                ))}
                        {isOutputPort && (
                            <Tooltip title="This asset is an output port for this Data Product" placement="topLeft">
                                <Icon size={30} background={ANTD_GRAY[4]} borderRadius={10}>
                                    <OutputIcon style={{ fontSize: 20 }} htmlColor={ANTD_GRAY[8]} />
                                </Icon>
                            </Tooltip>
                        )}
                    </LogoIcon>
                )}
            </>
        );
    };

    return <PlatformContentWrapper>{renderLogoIcon()}</PlatformContentWrapper>;
}
