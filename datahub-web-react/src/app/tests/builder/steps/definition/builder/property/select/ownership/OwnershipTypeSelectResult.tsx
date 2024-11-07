import React from 'react';
import styled from 'styled-components';
import { Divider } from 'antd';
import { Tooltip } from '@components';
import { OwnershipTypeEntity } from '../../../../../../../../../types.generated';
import { getOwnershipTypeDisplayName } from './utils';
import { ANTD_GRAY } from '../../../../../../../../entity/shared/constants';

const Container = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
`;

const Type = styled.div`
    color: ${ANTD_GRAY[8]};
`;

type Types = {
    ownershipType: OwnershipTypeEntity; // The selected ownership type
};

/**
 * Result when searching for a ownership type.
 */
export const OwnershipTypeSelectResult = ({ ownershipType }: Types) => {
    const displayName = getOwnershipTypeDisplayName(ownershipType);
    const stdType = ownershipType.info?.name;
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
