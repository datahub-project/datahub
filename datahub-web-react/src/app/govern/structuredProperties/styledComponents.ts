import { colors, Icon, typography } from '@src/alchemy-components';
import { Checkbox, Collapse, Divider, Drawer, Form, Modal, Select, Spin, Typography } from 'antd';
import styled from 'styled-components';

export const PageContainer = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    overflow: auto;
    margin: ${(props) => (props.$isShowNavBarRedesign ? '0' : '0 12px 12px 0')};
    padding: 16px 20px 20px 20px;
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    display: flex;
    flex: 1;
    flex-direction: column;
    gap: 20px;
    background-color: ${colors.white};
    ${(props) => props.$isShowNavBarRedesign && 'max-height: calc(100vh - 88px);'};
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        box-shadow: ${props.theme.styles['box-shadow-navbar-redesign']};
        margin: 5px;
    `}
`;

export const HeaderContainer = styled.div`
    display: flex;
    justify-content: space-between;
`;

export const TableContainer = styled.div`
    display: flex;
    overflow: auto;
    flex: 1;
`;

export const HeaderContent = styled.div`
    display: flex;
    flex-direction: column;
`;

export const ButtonContainer = styled.div`
    display: flex;
    align-self: center;
`;

export const DataContainer = styled.div`
    display: flex;
    flex-direction: column;
    width: calc(100% - 44px);
`;

export const PropName = styled(Typography.Text)`
    font-size: 14px;
    font-weight: 600;
    color: ${colors.gray[600]};
    line-height: normal;

    :hover {
        cursor: pointer;
        text-decoration: underline;
    }
`;

export const PropDescription = styled(Typography.Text)`
    font-size: 14px;
    font-weight: 400;
    color: ${colors.gray[1700]};
    line-height: normal;
`;

export const NameColumn = styled.div`
    display: flex;
    gap: 12px;
    align-items: center;
`;

export const IconContainer = styled.div`
    height: 32px;
    width: 32px;
    display: flex;
    justify-content: center;
    align-items: center;
    gap: 12px;
    border-radius: 200px;
    background-color: ${colors.gray[1000]};
`;

export const PillsContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

export const PillContainer = styled.div`
    display: flex;
`;

export const MenuItem = styled.div`
    display: flex;
    padding: 5px 100px 5px 5px;
    font-size: 14px;
    font-weight: 400;
    color: ${colors.gray[600]};
    font-family: ${typography.fonts.body};
`;

export const DrawerHeader = styled.div`
    display: flex;
    justify-content: space-between;
`;

export const TitleContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 6px;
`;

export const StyledIcon = styled(Icon)`
    &:hover {
        cursor: pointer;
    }
`;

export const FooterContainer = styled.div`
    width: 100%;
`;

export const RowContainer = styled.div`
    display: grid;
    grid-template-columns: 180px 1fr;
    align-items: center;
`;

export const ViewFieldsContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 24px;
`;

export const CheckboxWrapper = styled.div`
    display: flex;
    align-items: center;
    gap: 10px;
    margin: 10px 0;
    color: #374066;
    p {
        color: #374066;
        font-weight: 500;
    }
`;

export const StyledCheckbox = styled(Checkbox)`
    .ant-checkbox-checked .ant-checkbox-inner {
        background-color: ${colors.violet[500]};
        border-color: ${colors.violet[500]} !important;
    },
`;

export const StyledText = styled.div`
    display: inline-flex;
    margin-left: -4px;
`;

export const StyledFormItem = styled(Form.Item)`
    margin: 0;
`;

export const GridFormItem = styled(Form.Item)`
    display: grid;
`;

export const FieldLabel = styled.div`
    font-size: 14px;
    font-weight: 500;
    color: ${colors.gray[600]};
    margin-bottom: 24px;
`;

export const InputLabel = styled.div`
    font-size: 14px;
    font-weight: 500;
    color: ${colors.gray[600]};
    display: flex;
    gap: 2px;
`;

export const StyledLabel = styled.div`
    font-size: 12px;
    font-weight: 700;
    color: ${colors.gray[1700]};
`;

export const DescriptionContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

export const FlexContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
`;

export const StyledDrawer = styled(Drawer)`
    .ant-drawer-body {
        padding: 16px;
    }

    .ant-drawer-header {
        padding: 16px;
    }
`;

export const StyledCollapse = styled(Collapse)`
    .ant-collapse-header {
        padding: 0 !important;
    }

    .ant-collapse-content-box {
        padding: 12px 0 !important;
    }

    .ant-collapse-arrow {
        right: 0 !important;
    }
`;

export const CollapseHeader = styled.div`
    border-top: 1px solid ${colors.gray[1400]};
    padding: 16px 0;
    margin-left: -16px;
    width: calc(100% + 32px);
    padding: 16px;
    margin-top: 12px;
`;

export const TogglesContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 32px;
`;

export const StyledSpin = styled(Spin)`
    max-height: 100% !important;
    color: ${colors.violet[500]};
`;

export const CreatedByContainer = styled.div`
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 3px 6px 3px 4px;
    border-radius: 20px;
    border: 1px solid ${colors.gray[1400]};

    :hover {
        cursor: pointer;
    }
`;

export const SubTextContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;

    p {
        margin: 0;
    }
`;

export const ValueListContainer = styled.div`
    margin-bottom: 24px;
    display: flex;
    align-items: center;
    gap: 4px;
    font-size: 14px;
    color: ${colors.gray[500]};

    svg {
        :hover {
            cursor: pointer;
        }
    }
`;

export const ValueType = styled.div`
    background-color: ${colors.gray[100]};
    border-radius: 4px;
    padding: 2px 4px;
`;

export const StyledModal = styled(Modal)`
    font-family: Mulish;

    &&& .ant-modal-content {
        box-shadow: 0px 4px 12px 0px rgba(9, 1, 61, 0.12);
        border-radius: 12px;
        width: 452px;
    }

    .ant-modal-header {
        border-top-left-radius: 12px !important;
        border-top-right-radius: 12px !important;
        padding: 20px 20px 8px 20px;
    }

    .ant-modal-body {
        padding: 0;
    }
`;

export const FieldGroupContainer = styled.div`
    display: grid;
    margin-bottom: 8px;
`;

export const DeleteIconContainer = styled.div`
    display: flex;
    margin-top: -16px;
    justify-self: end;

    :hover {
        cursor: pointer;
    }
`;

export const ValuesContainer = styled.div<{ height: number }>`
    max-height: ${(props) => `calc(${props.height}px - 200px)`};
    overflow: auto;
    padding: 20px;
`;

export const ValuesList = styled.div`
    font-size: 14px;
    color: ${colors.gray[500]};
    display: flex;
    flex: 1;
    align-items: center;
    flex-wrap: wrap;

    p {
        line-height: 24px;
    }
`;

export const ItemsList = styled.div`
    font-size: 14px;
    display: flex;
    align-items: center;
    flex-wrap: wrap;
`;

export const ItemsContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 16px;
    width: fit-content;
    margin: 0 24px 24px 0;
`;

export const AddButtonContainer = styled.div`
    display: flex;
    margin: 10px 20px 0 0;
    justify-self: end;
`;

export const FormContainer = styled.div`
    display: grid;
`;

export const ModalFooter = styled.div`
    display: flex;
    gap: 16px;
    justify-content: end;
`;

export const VerticalDivider = styled(Divider)`
    color: ${colors.gray[1400]};
    height: 20px;
    width: 2px;
`;

export const StyledSelect = styled(Select)`
    font-family: ${typography.fonts.body};

    .ant-select-selector {
        height: 50px !important;
        border-radius: 8px !important;
        outline: 2px solid transparent !important;

        &:hover,
        &:focus-within {
            border: 1px solid ${colors.violet[200]} !important;
            outline: 2px solid ${colors.violet[200]} !important;
            box-shadow: none !important;
        }
    }

    .ant-select-selection-item {
        p {
            text-overflow: ellipsis;
            white-space: nowrap;
            overflow: hidden;
        }
    }

    .ant-select-selection-placeholder {
        display: flex;
        align-items: center;
        font-size: 14px;
        color: ${colors.gray[400]};
    }
`;

export const CustomDropdown = styled.div`
    .ant-select-item-option-content {
        white-space: normal;
    }
`;

export const SelectOptionContainer = styled.div`
    display: flex;
    flex-direction: column;
    padding: 4px 0;

    p {
        line-height: 20px;
    }
`;

export const StyledDivider = styled(Divider)`
    color: ${colors.gray[1400]};
    margin: 16px 0;
`;

export const ViewDivider = styled(Divider)`
    color: ${colors.gray[1400]};
    margin: 16px 0 0 -16px;
    width: calc(100% + 32px);
`;

export const EmptyContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    height: 100%;
    width: 100%;
    gap: 16px;

    svg {
        width: 160px;
        height: 160px;
    }
`;

export const CardIcons = styled.div`
    display: flex;
    justify-content: end;
    gap: 12px;

    div {
        border: 1px solid $E9EAEE;
        border-radius: 20px;
        width: 28px;
        height: 28px;
        padding: 4px;
        color: #8088a3;
        :hover {
            cursor: pointer;
        }
    }
`;
