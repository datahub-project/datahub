import { createGlobalStyle } from 'styled-components';

export const SelectCronGlobalStyles = createGlobalStyle`

    .react-js-cron-select-dropdown {

        .ant-select-item {
            color: ${(props) => props.theme.colors.textSecondary};
            font-size: 14px;
            font-weight: 400;
        }
    }
`;
