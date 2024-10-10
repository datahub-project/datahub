import React from 'react';
import styled from 'styled-components';
import { Link } from 'react-router-dom';
import { Typography } from 'antd';
import { EntityType, ParentDataset } from '../../../../../../../types.generated';
import { ANTD_GRAY } from '../../../../constants';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';

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
    parentDatasetUrn: string | '';
    parentDataset: ParentDataset;
    parentDatasetIcon: JSX.Element;
}

function DatasetLink(props: Props) {
    const { parentDatasetUrn, parentDataset, parentDatasetIcon } = props;
    const entityRegistry = useEntityRegistry();
    if (!parentDatasetUrn && !parentDataset) return null;

    const datasetUrl = entityRegistry.getEntityUrl(EntityType.Dataset, parentDatasetUrn);
    const datasetName = entityRegistry.getDisplayName(EntityType.Dataset, parentDataset);

    return (
        <StyledLink to={datasetUrl} data-testid="dataset">
            <DatasetIcon>{parentDatasetIcon}</DatasetIcon>
            <DatasetText>{datasetName}</DatasetText>
        </StyledLink>
    );
}

export default DatasetLink;
