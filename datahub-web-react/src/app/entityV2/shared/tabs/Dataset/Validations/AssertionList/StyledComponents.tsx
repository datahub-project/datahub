import { ANTD_GRAY } from '@src/app/entityV2/shared/constants';
import styled from 'styled-components';

export const StyledTableContainer = styled.div`
    table tr.acryl-selected-table-row {
        background-color: ${ANTD_GRAY[4]};
    }
    margin: 0px 12px 12px 12px;
`;
