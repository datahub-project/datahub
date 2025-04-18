import React from 'react';
import styled from 'styled-components';
import { Divider } from 'antd';
import { Tooltip } from '@components';
import { StructuredPropertyEntity } from '../../../../../../../../../types.generated';
import { getPropertyDisplayName } from './utils';
import { ANTD_GRAY } from '../../../../../../../../entity/shared/constants';

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
