import React, { useRef, useState } from 'react';
import styled from 'styled-components/macro';
import ColorThief from 'colorthief';
import { useHistory } from 'react-router';
import { Tooltip } from 'antd';
import { DataPlatform, EntityType } from '../../../../../../../types.generated';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { ANTD_GRAY } from '../../../../../../entity/shared/constants';
import { IconStyleType } from '../../../../../../entity/Entity';
import { navigateToSearchUrl } from '../../../../../../searchV2/utils/navigateToSearchUrl';
import { PLATFORM_FILTER_NAME } from '../../../../../../searchV2/utils/constants';
import { formatNumber, formatNumberWithoutAbbreviation } from '../../../../../../shared/formatNumber';
import { SEARCH_COLORS } from '../../../../../../entityV2/shared/constants';

const Card = styled.div`
    border-radius: 10px;
    background-color: #ffffff;
    padding: 16px;
    min-width: 180px;
    border: 2px solid transparent;
    :hover {
        border: 2px solid ${SEARCH_COLORS.LINK_BLUE};
        cursor: pointer;
    }
    display: flex;
    justify-content: start;
    align-items: center;
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

const Icon = styled.div<{ backgroundColor?: string }>`
    margin-right: 14px;
    border-radius: 12px;
    background-color: ${(props) => props.backgroundColor || ANTD_GRAY[3]};
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 6px;
    width: 50px;
    height: 50px;
`;

const PreviewImage = styled.img`
    height: 30px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
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
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const [platformBackground, setPlatformBackground] = useState<string | undefined>(undefined);
    const imgRef = useRef<HTMLImageElement>(null);
    const name = entityRegistry.getDisplayName(EntityType.DataPlatform, platform);
    const logo = platform?.properties?.logoUrl ? (
        <PreviewImage
            ref={imgRef}
            onLoad={() => {
                const colorThief = new ColorThief();
                const img = imgRef.current;
                if (img) {
                    img.crossOrigin = 'anonymous';
                }
                const result = colorThief.getColor(img, 25);
                setPlatformBackground(`rgb(${result[0]}, ${result[1]}, ${result[2]}, .1)`);
            }}
            src={platform?.properties?.logoUrl || undefined}
            alt={name}
        />
    ) : (
        entityRegistry.getIcon(EntityType.DataPlatform, 12, IconStyleType.ACCENT)
    );

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
            <Card key={platform.urn} onClick={navigateToPlatformSearch}>
                <Icon backgroundColor={platformBackground}>{logo}</Icon>
                <Text>
                    <Name>{name}</Name>
                    {(count !== undefined && <Count>{formatNumber(count)}</Count>) || null}
                </Text>
            </Card>
        </Tooltip>
    );
};
