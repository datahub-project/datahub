import { Menu } from 'antd';
import styled from 'styled-components';

const NavBarMenuItemGroup = styled(Menu.ItemGroup)`
    .ant-menu-item-group-title {
        margin-top: 8px;
        padding: 8px 0;
        color: #8088a3;
        font-family: Mulish;
        font-size: 14px;
        font-style: normal;
        font-weight: 700;
        line-height: normal;
        min-height: 38px;

        @media (max-height: 970px) {
            margin-top: 2px;
        }
        @media (max-height: 890px) {
            margin-top: 0px;
        }
        @media (max-height: 835px) {
            min-height: 34px;
        }
        @media (max-height: 800px) {
            min-height: 24px;
        }
        @media (max-height: 775px) {
            min-height: 14px;
        }
        @media (max-height: 750px) {
            min-height: 0px;
            padding: 4px 0;
        }
        @media (max-height: 730px) {
            min-height: 0px;
            padding: 0;
        }
    }
`;

export default NavBarMenuItemGroup;
