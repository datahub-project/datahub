import { RightOutlined } from '@ant-design/icons';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';

export const ContextPathSeparator = styled(RightOutlined)`
    color: ${ANTD_GRAY[6]};
    font-size: 10px;
    font-weight: normal;
    padding: 0px 4px 0px 4px;
`;
