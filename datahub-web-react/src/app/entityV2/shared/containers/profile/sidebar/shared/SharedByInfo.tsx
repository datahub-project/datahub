import { Tooltip } from '@components';
import InfoIcon from '@mui/icons-material/Info';
import React from 'react';
import styled from 'styled-components';

const Icon = styled.div`
    display: flex;
    align-items: center;
    color: #8c7ee0;
    svg {
        font-size: 20px;
    }
`;

export default function SharedByInfo() {
    return (
        <Icon>
            <Tooltip
                title="This asset was shared along with another asset. This asset could be shared by lineage or due to another relationship to the original shared asset."
                placement="left"
            >
                <InfoIcon />
            </Tooltip>
        </Icon>
    );
}
