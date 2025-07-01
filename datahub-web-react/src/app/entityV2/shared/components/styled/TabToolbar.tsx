import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';

export default styled.div`
    display: flex;
    position: relative;
    z-index: 1;
    justify-content: space-between;
    height: 46px;
    border-bottom: 1px solid ${ANTD_GRAY[4.5]};
    padding: 8px 16px;
    box-shadow: 0px 2px 6px 0px #0000000d;
    flex: 0 0 auto;
`;
