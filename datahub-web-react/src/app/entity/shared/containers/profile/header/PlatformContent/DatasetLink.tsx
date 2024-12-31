import React from 'react';
import styled from 'styled-components';
import { Link } from 'react-router-dom';
import { Typography } from 'antd';
import { Dataset, EntityType } from '../../../../../../../types.generated';
import { ANTD_GRAY } from '../../../../constants';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { IconStyleType } from '../../../../../Entity';

const DatasetText = styled(Typography.Text)`
    font-size: 12px;
    line-height: 20px;
    color: ${ANTD_GRAY[7]};
`;

export const DatasetIcon = styled.span`
    color: ${ANTD_GRAY[7]};
    margin-right: 4px;
    font-size: 12px;
`;

const StyledLink = styled(Link)`
    white-space: nowrap;
`;

interface Props {
    parentDataset: Dataset;
}

function DatasetLink(props: Props) {
    const { parentDataset } = props;
    const entityRegistry = useEntityRegistry();
    if (!parentDataset) return null;

    const datasetUrl = entityRegistry.getEntityUrl(EntityType.Dataset, parentDataset.urn);
    const datasetName = entityRegistry.getDisplayName(EntityType.Dataset, parentDataset);

    return (
        <StyledLink to={datasetUrl} data-testid="dataset">
            <DatasetIcon>{entityRegistry.getIcon(EntityType.Dataset, 14, IconStyleType.ACCENT)}</DatasetIcon>
            <DatasetText>{datasetName}</DatasetText>
        </StyledLink>
    );
}

export default DatasetLink;
