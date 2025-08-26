import { Tooltip } from '@components';
import { Divider } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { getOwnershipTypeDisplayName } from '@app/tests/builder/steps/definition/builder/property/select/ownership/utils';

import { OwnershipTypeEntity } from '@types';

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
