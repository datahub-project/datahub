import { Tooltip } from '@components';
import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { getPropertyDisplayName } from '@app/sharedV2/queryBuilder/builder/property/select/structured/utils';

import { StructuredPropertyEntity } from '@types';

const Container = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
`;

const Type = styled.div`
    color: ${ANTD_GRAY[8]};
`;

type Props = {
    property: StructuredPropertyEntity; // The selected property
};

/**
 * Result when searching for a structured property.
 */
export const StructuredPropertySelectResult = ({ property }: Props) => {
    const displayName = getPropertyDisplayName(property);
    const stdType = property.definition?.valueType?.info?.displayName;
    return (
        <Tooltip title={displayName}>
            <Container>
                {displayName}
                <Divider type="vertical" />
                <Type>{stdType}</Type>
            </Container>
        </Tooltip>
    );
};
