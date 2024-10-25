import React from 'react';
import styled from 'styled-components';
import { DataPlatform } from '../../../../../../../types.generated';
import StackPlatformImages from './StackPlatformImages';
import PlatformIcon from '../../../../../../sharedV2/icons/PlatformIcon';

const LogoIcon = styled.span`
    display: flex;
    gap: 4px;
    margin-right: 4px;
`;

const PlatformContentWrapper = styled.div`
    display: flex;
    align-items: center;
    margin: 0px 8px 0px 0;
    flex-wrap: nowrap;
`;

const iconStyles = {
    borderRadius: '16px',
    border: '1px solid #FFF',
    padding: '10px',
};

interface Props {
    platform?: DataPlatform;
    platforms?: DataPlatform[];
    size?: number;
}

function PlatformHeaderIcons(props: Props) {
    const { platform, platforms, size = 28 } = props;

    return (
        <PlatformContentWrapper>
            {platform && (
                <LogoIcon>
                    {!platforms && <PlatformIcon platform={platform} size={size} styles={iconStyles} />}
                    {!!platforms && <StackPlatformImages platforms={platforms} />}
                </LogoIcon>
            )}
        </PlatformContentWrapper>
    );
}

export default PlatformHeaderIcons;
