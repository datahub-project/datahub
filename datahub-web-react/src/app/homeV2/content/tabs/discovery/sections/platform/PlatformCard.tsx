import { Tooltip } from '@components';
import React from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components/macro';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { SEARCH_COLORS } from '@app/entityV2/shared/constants';
import { PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';
import { navigateToSearchUrl } from '@app/searchV2/utils/navigateToSearchUrl';
import { formatNumber, formatNumberWithoutAbbreviation } from '@app/shared/formatNumber';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { colors } from '@src/alchemy-components';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

import { DataPlatform, EntityType } from '@types';

const Card = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    border-radius: 10px;
    background-color: #ffffff;
    padding: 16px;
    min-width: 180px;
    border: ${(props) => (props.$isShowNavBarRedesign ? `1px solid ${colors.gray[100]}` : '2px solid transparent')};
    ${(props) => props.$isShowNavBarRedesign && 'border-radius: 8px;'}
    :hover {
        border: ${(props) => (props.$isShowNavBarRedesign ? '1px' : '2px')} solid ${SEARCH_COLORS.LINK_BLUE};
        cursor: pointer;
    }
    display: flex;
    justify-content: start;
    align-items: center;
    gap: 14px;
`;

const Text = styled.div``;

const Name = styled.div`
    font-size: 16px;
    color: ${ANTD_GRAY[7]};
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 160px;
    white-space: nowrap;
`;

const Count = styled.div`
    font-size: 16px;
    color: #56668e;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

type Props = {
    platform: DataPlatform;
    count?: number;
};

export const PlatformCard = ({ platform, count }: Props) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const name = entityRegistry.getDisplayName(EntityType.DataPlatform, platform);

    const navigateToPlatformSearch = () => {
        navigateToSearchUrl({
            history,
            filters: [
                {
                    field: PLATFORM_FILTER_NAME,
                    values: [platform.urn],
                },
            ],
        });
    };

    return (
        <Tooltip
            title={`View ${formatNumberWithoutAbbreviation(count)} ${name} assets`}
            showArrow={false}
            placement="bottom"
        >
            <Card key={platform.urn} onClick={navigateToPlatformSearch} $isShowNavBarRedesign={isShowNavBarRedesign}>
                <PlatformIcon
                    platform={platform}
                    size={30}
                    styles={{
                        padding: '10px',
                    }}
                />
                <Text>
                    <Name>{name}</Name>
                    {(count !== undefined && <Count>{formatNumber(count)}</Count>) || null}
                </Text>
            </Card>
        </Tooltip>
    );
};
