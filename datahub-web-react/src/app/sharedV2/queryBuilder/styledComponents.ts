import { Collapse, Select } from 'antd';
import styled from 'styled-components/macro';

import { Button, colors } from '@src/alchemy-components';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';

export const ConditionContainer = styled.div<{ depth: number }>`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 4px 16px;
    padding-left: ${(props) => props.depth * 20 + 50 + 8}px;
`;

export const SelectContainer = styled.div`
    display: flex;
    gap: 16px;
`;

export const IconsContainer = styled.div`
    display: flex;
`;

export const ToolbarContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 8px 16px;
`;

export const OperationButton = styled(Button)<{ isSelected: boolean }>`
    color: ${(props) => (props.isSelected ? colors.violet : colors.gray[600])};
    background-color: ${(props) => (props.isSelected ? colors.gray[1000] : 'transparent')};
    padding: 10px 12px;

    &:focus {
        background-color: ${(props) => (props.isSelected ? colors.gray[1000] : 'transparent')};
        box-shadow: none;
    }
`;

export const ActionsContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 16px;
`;

export const StyledCollapse = styled(Collapse)<{ depth: number; hasChildren: boolean; isExpanded: boolean }>`
    .ant-collapse-header {
        padding: 0 0 0 ${(props) => props.depth * 20 + (props.hasChildren ? 20 : 48)}px !important;
        align-items: center !important;
        margin: 8px 0;
        background-color: ${colors.gray[1500]};
    }

    .ant-collapse-item {
        position: relative;
    }

    .ant-collapse-item::after {
        content: ${(props) => (props.hasChildren && props.isExpanded ? '""' : 'none')};
        position: absolute;
        left: ${(props) => props.depth * 20 + 32}px;
        top: 52px;
        width: 2px;
        height: calc(100% - 78px);
        background-color: ${colors.gray[1400]};
        z-index: 1;
    }

    .ant-collapse-item::before {
        content: ${(props) => (props.hasChildren && props.isExpanded ? '""' : 'none')};
        position: absolute;
        left: ${(props) => props.depth * 20 + 34}px;
        top: calc(100% - 28px);
        width: 5px;
        height: 2px;
        background-color: ${colors.gray[1400]};
        z-index: 1;
    }

    .ant-collapse-content-box {
        padding: 0 !important;
    }

    .ant-collapse-arrow {
        margin-right: 0 !important;
    }
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

export const StyledSelect = styled(Select)`
    min-width: 200px;
    display: flex;
    align-items: center;
`;
