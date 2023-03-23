import React from 'react';
import { Image, Tooltip, Typography } from 'antd';
import styled from 'styled-components/macro';
import useGetSourceLogoUrl from '../../../../ingest/source/builder/useGetSourceLogoUrl';
import { capitalizeFirstLetter } from '../../../../shared/textUtil';
import { ANTD_GRAY } from '../../../../entity/shared/constants';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import { IconStyleType } from '../../../../entity/Entity';

const EntityColumnContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const PreviewImage = styled(Image)`
    max-height: 28px;
    width: auto;
    object-fit: contain;
    margin: 0px;
    background-color: transparent;
`;

const EntityNameContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

const EntityTypeContainer = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: 4px;
`;

const EntityTypeText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 14px;
    line-height: 20px;
    font-weight: 400;
    color: ${ANTD_GRAY[8]};
`;

const EntityNameText = styled(Typography.Text)`
    font-family: 'Manrope', sans-serif;
    font-size: 16px;
    line-height: 24px;
    font-weight: 500;
`;

interface Props {
    record: any;
}

export function EntityColumn({ record }: Props) {
    const entityRegistry = useEntityRegistry();
    const platformType = record.platform;
    const iconUrl = useGetSourceLogoUrl(platformType);
    const typeDisplayName = capitalizeFirstLetter(platformType);
    const typeIcon = entityRegistry.getIcon(record.entityType, 14, IconStyleType.ACCENT);

    return (
        <EntityColumnContainer>
            {iconUrl ? (
                <Tooltip overlay={typeDisplayName}>
                    <PreviewImage preview={false} src={iconUrl} alt={platformType || ''} />
                </Tooltip>
            ) : (
                <EntityTypeText>{typeDisplayName}</EntityTypeText>
            )}
            <EntityNameContainer>
                <EntityTypeContainer>
                    {typeIcon}
                    <EntityTypeText>{record.entityType}</EntityTypeText>
                </EntityTypeContainer>
                <EntityNameText>{record.entityName}</EntityNameText>
            </EntityNameContainer>
        </EntityColumnContainer>
    );
}
