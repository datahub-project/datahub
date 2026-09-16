import { Menu } from 'antd';
import styled from 'styled-components';

const NavBarMenuItemGroup = styled(Menu.ItemGroup)<{ $isCollapsed?: boolean }>`
    .ant-menu-item-group-title {
        display: ${(props) => (props.$isCollapsed ? 'none' : 'block')};
        margin-top: ${(props) => (props.$isCollapsed ? '0' : '4px')};
        padding: ${(props) => (props.$isCollapsed ? '0' : '4px 0')};
        min-height: ${(props) => (props.$isCollapsed ? '0' : '28px')};
        color: ${(props) => props.theme.colors.textTertiary};
        font-family: Mulish;
        font-size: 14px;
        font-style: normal;
        font-weight: 700;
        line-height: normal;

        @media (max-height: 970px) {
            margin-top: ${(props) => (props.$isCollapsed ? '0' : '2px')};
        }
        @media (max-height: 890px) {
            margin-top: 0px;
        }
        @media (max-height: 835px) {
            min-height: ${(props) => (props.$isCollapsed ? '0' : '24px')};
        }
        @media (max-height: 800px) {
            min-height: ${(props) => (props.$isCollapsed ? '0' : '20px')};
        }
        @media (max-height: 775px) {
            min-height: ${(props) => (props.$isCollapsed ? '0' : '12px')};
        }
        @media (max-height: 750px) {
            min-height: ${(props) => (props.$isCollapsed ? '0' : '0px')};
            padding: ${(props) => (props.$isCollapsed ? '0' : '2px 0')};
        }
        @media (max-height: 730px) {
            min-height: ${(props) => (props.$isCollapsed ? '0' : '0px')};
            padding: ${(props) => (props.$isCollapsed ? '0' : '0')};
        }
    }
`;

export default NavBarMenuItemGroup;
