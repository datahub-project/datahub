import { Form, Table } from 'antd';
import styled, { keyframes } from 'styled-components';
import { ANTD_GRAY, REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { Button, colors } from '@src/alchemy-components';

export const IncidentListStyledTable = styled(Table)`
    max-width: none;
    &&& .ant-table-thead .ant-table-cell {
        font-weight: 600;
        font-size: 12px;
        color: ${ANTD_GRAY[8]};
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    &&& .ant-table-expanded-row > .ant-table-cell {
        padding-left: 0px;
    }
    &&& .ant-table-tbody > tr > td > .ant-table-wrapper:only-child .ant-table,
    .ant-table-tbody > tr > td > .ant-table-expanded-row-fixed > .ant-table-wrapper:only-child .ant-table {
        margin-left: 0px;
    }
    &&
        .ant-table-thead
        > tr
        > th:not(:last-child):not(.ant-table-selection-column):not(.ant-table-row-expand-icon-cell):not(
            [colspan]
        )::before {
        border: 1px solid ${ANTD_GRAY[4]};
    }
    &&& .ant-table-thead > tr > th {
        line-height: 5px;
    }
    &&& .ant-table-cell {
        background-color: transparent;
    }

    &&& .acryl-selected-incidents-table-row {
        background-color: ${ANTD_GRAY[4]};
    }

    .group-header {
        cursor: pointer;
        background-color: ${ANTD_GRAY[3]};
    }
    &&& .acryl-incidents-table-row {
        cursor: pointer;
        background-color: ${ANTD_GRAY[2]};
        :hover {
            background-color: ${ANTD_GRAY[3]};
        }
    }
`;

export const ListContainer = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 5px;
    overflow: auto;
    @media (max-width: 768px) {
        max-width: 50vw;
    }
    @media (max-width: 1024px) {
        max-width: 35vw;
    }
    @media (max-width: 1440px) {
        max-width: 15vw;
    }
    @media (max-width: 1680px) {
        max-width: 22vw;
    }
    @media (max-width: 1920px) {
        max-width: 20vw;
    }
    @media (min-width: 1920px) {
        max-width: 15vw;
    }
`;

export const ListItemContainer = styled.div`
    display: flex;
    border-radius: 200px;
    padding: 4px;
    min-width: fit-content;
`;

export const DescriptionSection = styled.div`
    margin-bottom: 30px;
`;

export const DetailsSection = styled.div`
    margin-bottom: 30px;
    line-height: 17.57px;
    display: flex;
    align-items: center;
`;

export const DetailsLabel = styled.div`
    margin-right: 60px;
    width: 20%;
    font-weight: 600;
    color: ${colors.gray[1700]};
    font-size: 12px;
`;

export const ActivitySection = styled.div`
    display: flex;
    flex-direction: column;
`;

export const ActivityLabelSection = styled.div`
    font-weight: 500;
    font-size: 18px;
    color: ${colors.gray[600]};
    padding: 8px 4px;
    margin-bottom: 8px;
`;

export const TimelineWrapper = styled.div`
    margin-left: 12px;
    margin-top: 16px;
`;

export const ContentRow = styled.div`
    display: flex;
    flex-direction: column;
`;

export const Content = styled.div`
    position: relative;
    display: flex;
    flex-direction: column;
    gap: 0px;
    top: -8px;
    margin-left: 11px;
`;

export const ActivityStatusText = styled.div`
    font-size: 14px;
    color: ${colors.gray[600]};
    font-weight: 500;
    a {
        color: inherit;
        &:hover {
            color: inherit;
        }
    }
`;

export const Header = styled.div`
    display: flex;
    align-items: center;
    cursor: pointer;
    margin-bottom: 1rem;
`;

export const ToggleIcon = styled.span`
    color: #666;
`;

export const Divider = styled.div`
    border-top: 1px solid #e0e0e0;
    margin: 16px 0;
`;

export const Container = styled.div`
    padding: 20px;
    width: 100%;
`;

export const Text = styled.div`
    font-size: 12px;
    color: #0066cc;
    text-decoration: underline;
    &&:hover {
        cursor: pointer;
    }
`;

export const CategoryText = styled.div`
    font-size: 14px;
    font-weight: 400;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
`;

export const SelectFormItem = styled(Form.Item)<{ customStyle?: React.CSSProperties }>`
    width: auto !important;
    .ant-form-item-row {
        display: flex !important;
        flex-direction: ${({ customStyle }) => customStyle?.flexDirection || 'row !important'};
        justify-content: space-between;
        align-items: ${({ customStyle }) => customStyle?.alignItems || 'center'};
        flex-wrap: nowrap;
    }
    .ant-form-item-control {
        width: auto !important;
    }
    .ant-form-item-label {
        min-width: 25%;
    }
    .ant-form-item-label > label {
        color: ${({ customStyle }) => customStyle?.color || `${colors.gray[600]} !important`};
    }
    .ant-form-item-label > label.ant-form-item-required::before {
        content: none;
    }
`;

export const StyledForm = styled(Form)`
    max-width: 600px;
`;

export const InputFormItem = styled(Form.Item)`
    margin-bottom: 16px;
    font-size: large;
    font-weight: 500;
    line-height: 20.08px;
    text-align: left;
    .ant-form-item-label > label {
        color: ${colors.gray[600]} !important;
    }
`;

export const SaveButton = styled(Button)<{ disabled: boolean }>`
    width: 100%;
    height: 36px;
    border-radius: 4px;
    border: 1px solid ${colors.violet[500]};
    background-color: ${({ disabled }) => (disabled ? '#f9fafc' : colors.violet[500])};
    font-size: 16px;
    font-weight: 600;
    line-height: 22.59px;
    justify-content: center;
    color: ${({ disabled }) => (disabled ? '#8088a3' : '#ffffff')};
`;

export const IncidentFooter = styled.div`
    width: 100%;
    padding: 16px;
    border-top: 1px solid #e9eaee;
    box-shadow: 0px 0px 6px 0px #5d668b33;
    max-height: 68px;
    position: absolute;
    bottom: 0;
    background-color: white;
`;

export const StyledFormElements = styled.div`
    margin: 16px;
    overflow-y: auto;
    overflow-x: hidden;
    height: 84vh;
    font-size: large;
    font-weight: 500;
    line-height: 20.08px;
    text-align: left;
    @media (max-width: 1024px) {
        height: 80vh;
    }
    @media (max-width: 1440px) {
        height: 84vh;
    }
    padding: 4px;
`;

export const SelectWrapper = styled.div``;
export const AssetWrapper = styled.div`
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 4px;
`;

export const LoadingWrapper = styled.div`
    display: flex;
    justify-content: center;
    margin: 5px;
`;

export const LinkedAssets = styled.div`
    display: flex;
    gap: 5px;
    flex-wrap: wrap;
`;

export const StyledHeader = styled.div`
    display: flex;
    height: 64px;
    align-items: center;
    padding: 16px;
    justify-content: space-between;
    box-shadow: 0px 0px 6px 0px #5d668b33;
`;

export const StyledHeaderActions = styled.div`
    display: flex;
    gap: 20px;
    align-items: center;
    color: ${colors.gray[1800]};
    cursor: pointer;
`;

export const StyledTitle = styled.span`
    font-size: large;
    font-weight: 700;
    line-height: 20.08px;
    text-align: left;
    color: ${colors.gray[600]};
`;

const spin = keyframes`
  100% {
    transform: rotate(360deg);
  }
`;

export const StyledSpinner = styled.div`
    width: 16px;
    height: 16px;
    margin-right: 8px;
    border: 2px solid #8088a3;
    color: #8088a3;
    border-top: 2px solid transparent;
    border-radius: 50%;
    animation: ${spin} 0.8s linear infinite;
    display: inline-block;
`;
