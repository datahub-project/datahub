import { createGlobalStyle } from 'styled-components';

import { colors } from '@components/theme';

export const SelectCronGlobalStyles = createGlobalStyle`

    .react-js-cron-select-dropdown {

        .ant-select-item {
            color: ${colors.gray[500]};
            font-size: 14px;
            font-weight: 400;
        }
    }
`;
