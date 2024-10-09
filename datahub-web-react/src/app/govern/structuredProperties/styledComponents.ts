import { colors, Icon, typography } from '@src/alchemy-components';
import { Collapse, Divider, Drawer, Input, Modal, Select, Spin, Typography } from 'antd';
import styled from 'styled-components';

export const PageContainer = styled.div`
    overflow: auto;
    margin: 0 12px 12px 0;
    padding: 16px;
    border-radius: 8px;
    display: flex;
    flex: 1;
    flex-direction: column;
    gap: 20px;
    background-color: ${colors.white};
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
    color: ${colors.gray[1600]};
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

export const StyledSearch = styled(Input.Search)`
    height: 40px;
    width: 272px;

    .ant-input-wrapper {
        .ant-input-affix-wrapper {
            height: 40px;
            border-color: ${colors.gray[1400]};
            box-shadow: none;
            border-right: none;

            &:hover,
            &:focus {
                border-color: ${colors.gray[1400]};
            }

            input {
                color: ${colors.gray[600]};
            }
        }

        button {
            height: 40px;
            width: 40px;
            border-color: ${colors.gray[1400]};
            border-left: none;
            box-shadow: none;

            &:hover {
                border-color: ${colors.gray[1400]};
            }
        }
    }
`;

export const DrawerHeader = styled.div`
    display: flex;
    justify-content: space-between;
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

export const FlexContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
`;

export const StyledDrawer = styled(Drawer)`
    .ant-drawer-body {
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
    border-bottom: 1px solid ${colors.gray[1400]};
    padding: 16px 0;
    margin-left: -16px;
    width: calc(100% + 32px);
    padding: 16px;
`;

export const StyledSpin = styled(Spin)`
    max-height: 100% !important;
    color: ${colors.violet[500]};
`;

export const CreatedByContainer = styled.div`
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 3px 6px;
    border-radius: 20px;
    border: 1px solid ${colors.gray[1400]};
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

    svg {
        :hover {
            cursor: pointer;
        }
    }
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

export const ValuesContainer = styled.div`
    max-height: 400px;
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

export const ItemsContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 16px;
    width: fit-content;
    margin: 0 24px 24px 0;
`;

export const AddButtonContainer = styled.div`
    display: flex;
    margin: 20px;
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

        &:hover,
        &:focus-within {
            border: 3px solid ${colors.violet[200]} !important;
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
