import { ExclamationCircleOutlined } from '@ant-design/icons';
import { colors, Icon } from '@src/alchemy-components';
import theme from '@src/alchemy-components/theme';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { WARNING_COLOR_HEX } from '@src/app/entityV2/shared/tabs/Incident/incidentUtils';
import { applyOpacity } from '@src/app/shared/styleUtils';
import { Checkbox, Divider, Modal, Radio, Select, Spin, Tag, Typography } from 'antd';
import styled from 'styled-components';

export const CreateFormContainer = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    display: flex;
    flex-direction: column;
    gap: ${(props) => (props.$isShowNavBarRedesign ? '6px' : '20px')};
    height: 100%;
`;

export const ContentContainer = styled.div<{ $showV1Styles?: boolean; $isShowNavBarRedesign?: boolean }>`
    display: flex;
    flex-direction: column;
    padding: 16px 0;

    ${(props) => !props.$isShowNavBarRedesign && 'margin-right: 16px;'}
    ${(props) => !props.$showV1Styles && props.$isShowNavBarRedesign && 'margin: 5px;'}
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '12px'};
    flex: 1;
    background-color: ${colors.white};
    gap: 12px;
    max-height: calc(100vh - 152px);
    overflow: auto;
    box-shadow: ${(props) =>
        props.$isShowNavBarRedesign
            ? props.theme.styles['box-shadow-navbar-redesign']
            : '0px 0px 24px 0px rgba(0, 0, 0, 0.1)'};

    ${(props) => props.$showV1Styles && `margin: 8px 24px 0 24px;`}
`;

export const ContentWrapper = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    overflow: auto;
    padding: 0 ${(props) => (props.$isShowNavBarRedesign ? '20px' : '16px')};
`;

export const FooterContainer = styled.div<{ $showV1Styles?: boolean; $isShowNavBarRedesign?: boolean }>`
    display: flex;
    align-items: center;
    justify-content: flex-end;
    gap: 20px;
    margin-right: 16px;
    height: 64px;
    padding: ${(props) => (props.$isShowNavBarRedesign ? '16px 20px' : '16px')};
    background-color: ${colors.white};
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '12px 12px 0px 0px'};
    box-shadow: ${(props) =>
        props.$isShowNavBarRedesign
            ? props.theme.styles['box-shadow-navbar-redesign']
            : '0px 0px 24px 0px rgba(0, 0, 0, 0.1)'};

    ${(props) => props.$showV1Styles && `margin: 0 24px;`}
    ${(props) => props.$isShowNavBarRedesign && 'margin: 5px;'}
`;

export const AddElementContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

export const LeftSection = styled.div`
    display: flex;
    flex-direction: column;
`;

export const OwnershipCheckbox = styled.div`
    display: flex;
    align-items: center;
    gap: 10px;
    margin: 10px 0;
`;

export const FormFieldsContainer = styled.div`
    display: flex;
    flex-direction: column;
    max-width: 412px;
    margin-top: 20px;
`;

export const FieldLabel = styled(Typography.Text)`
    font-size: 14px;
    font-weight: 500;
    color: ${colors.gray[600]};
    margin-bottom: 4px;
    display: block;
`;

export const BreadcrumbContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    margin: 12px 0 16px 0;
    &&& {
        font-size: 16px;
    }
`;

export const Header = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

export const BackText = styled.div`
    :hover {
        cursor: pointer;
    }
`;

export const StyledSpin = styled(Spin)`
    max-height: 100% !important;
    color: ${colors.violet};
`;

export const StyledSelect = styled(Select)`
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

export const OptionsContainer = styled.div`
    display: flex;
    flex-direction: column;
    padding: 12px;
    gap: 10px;
`;

export const StyledModal = styled(Modal)`
    font-family: Mulish;

    &&& .ant-modal-content {
        box-shadow: 0px 4px 12px 0px rgba(9, 1, 61, 0.12);
        border-radius: 12px;
        width: 452px;
    }

    .ant-modal-header {
        border-radius: 12px !important;
    }

    .ant-modal-body {
        padding: 0 20px;
    }

    label {
        font-size: 14px !important;
    }
`;

export const StyledRadioGroup = styled(Radio.Group)`
    span {
        color: ${colors.gray[1600]};
    }
    .ant-radio-checked .ant-radio-inner {
        border-color: ${theme.semanticTokens.colors.primary} !important;
    }

    .ant-radio-checked .ant-radio-inner:after {
        background-color: ${theme.semanticTokens.colors.primary};
    }

    .ant-radio:hover .ant-radio-inner {
        border-color: ${theme.semanticTokens.colors.primary};
    }
`;

export const ModalFooter = styled.div`
    display: flex;
    justify-content: space-between;
`;

export const FooterButtonsContainer = styled.div`
    display: flex;
    gap: 16px;
    justify-content: end;
`;

export const RequiredFieldContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
`;

export const CardsList = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
    margin: 10px 0;
`;

export const CardContainer = styled.div<{ isDragging: boolean; transform?: string }>`
    display: flex;
    justify-content: space-between;
    align-items: center;
    border: 1px solid ${colors.gray[100]};
    border-radius: 4px;
    padding: 20px;
    background-color: ${colors.white};
    box-shadow: ${(props) => (props.isDragging ? '0 4px 12px rgba(0, 0, 0, 0.2)' : 'none')};
    cursor: ${(props) => (props.isDragging ? 'grabbing' : 'inherit')};
    z-index: ${(props) => (props.isDragging ? '999' : 'auto')};
    transform: ${(props) => props.transform};
`;

export const DragIcon = styled(Icon)<{ isDragging: boolean }>`
    cursor: ${(props) => (props.isDragging ? 'grabbing' : 'grab')};
`;

export const MandatoryTag = styled(Tag)`
    background-color: #f1fbfe;
    border: 1px solid #f1fbfe !important;
    color: #09739a;
    font-size: 14px;
    font-weight: 600;
    padding: 3px 10px;
`;

export const CardData = styled.div<{ width: string }>`
    width: ${(props) => props.width};
`;

export const CardIcons = styled.div`
    display: flex;
    justify-content: end;
    gap: 12px;

    div {
        border: 1px solid ${REDESIGN_COLORS.SILVER_GREY};
        border-radius: 20px;
        width: 28px;
        height: 28px;
        padding: 4px;
        color: ${REDESIGN_COLORS.GREY_300};
        :hover {
            cursor: pointer;
        }
    }
`;

export const NameColumn = styled.div`
    display: flex;
    align-items: center;
    gap: 12px;
`;

export const VerticalFlexBox = styled.div`
    display: flex;
    flex-direction: column;
`;

export const ItemDivider = styled(Divider)`
    margin: 0;
`;

export const ListContainer = styled.div`
    display: flex;
    flex-direction: column;
    margin: 16px 0;
`;

export const ListItem = styled.div`
    display: flex;
    align-items: center;
    margin: 12px 20px;
    justify-content: space-between;
`;

export const StyledTag = styled(Tag)`
    padding: 0px 7px 0px 0px;
    margin: 2px;
    display: flex;
    justify-content: start;
    align-items: center;
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

export const StyledCheckbox = styled(Checkbox)<{ checked?: boolean; indeterminate?: boolean; disabled?: boolean }>`
    .ant-checkbox-inner {
        border: 1px solid ${colors.gray[300]} !important;
        border-radius: 3px;
    }
    ${(props) =>
        props.checked &&
        !props.indeterminate &&
        `
    .ant-checkbox-inner {
        background-color: ${theme.semanticTokens.colors.primary};
        border-color: ${theme.semanticTokens.colors.primary} !important;
    }
`}
    ${(props) =>
        props.indeterminate &&
        `
    .ant-checkbox-inner {
        &:after {
            background-color: ${theme.semanticTokens.colors.primary};
        }
    }
`}
${(props) =>
        props.disabled &&
        `
    .ant-checkbox-inner {
        background-color: ${colors.gray[200]} !important;
    }
`}
`;

export const StyledLabel = styled.label`
    color: ${colors.gray[1600]};
    margin-left: 8px;
`;

export const WarningWrapper = styled.div`
    background-color: ${applyOpacity(WARNING_COLOR_HEX, 8)};
    border: 1px solid ${applyOpacity(WARNING_COLOR_HEX, 20)};
    border-radius: 4px;
    padding: 6px 4px;
    font-size: 14px;
    color: ${colors.gray[1600]};
    margin: 0 0 20px 0;
    display: flex;
    align-items: center;
`;

export const StyledExclamationOutlined = styled(ExclamationCircleOutlined)`
    color: ${WARNING_COLOR_HEX};
    font-size: 16px;
    margin-right: 8px;
    margin-left: 4px;
`;

export const AllowedItemsWrapper = styled.div`
    margin-bottom: 24px;
`;

export const SelectorWrapper = styled.div`
    margin-top: 12px;
    margin-left: 24px;
`;
