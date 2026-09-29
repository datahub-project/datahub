import styled from 'styled-components/macro';

import { Button } from '@src/alchemy-components';

export const ConditionContainer = styled.div<{ depth: number }>`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 4px 16px;
    padding-left: ${(props) => props.depth * 20 + 50 + 8}px;
`;

export const ConditionElementWithFixedWidth = styled.div`
    width: 175px;
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
    flex: 1;
    justify-content: space-between;
    align-items: center;
    padding: 8px 16px;
`;

export const OperationButton = styled(Button)<{ isSelected: boolean }>`
    color: ${(props) => (props.isSelected ? props.theme.colors.textSelected : props.theme.colors.textSecondary)};
    background-color: ${(props) => (props.isSelected ? props.theme.colors.bgSelected : 'transparent')};
    padding: 10px 12px;

    &:focus {
        background-color: ${(props) => (props.isSelected ? props.theme.colors.bgSelected : 'transparent')};
        box-shadow: none;
    }
`;

export const ActionsContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 16px;
`;

export const QueryGroup = styled.div<{ $depth: number; $hasChildren: boolean; $isExpanded: boolean }>`
    position: relative;

    &::after {
        content: ${(props) => (props.$hasChildren && props.$isExpanded ? '""' : 'none')};
        position: absolute;
        left: ${(props) => props.$depth * 20 + 26}px;
        top: 52px;
        width: 2px;
        height: calc(100% - 78px);
        background-color: ${(props) => props.theme.colors.bgSurface};
        z-index: 1;
    }

    &::before {
        content: ${(props) => (props.$hasChildren && props.$isExpanded ? '""' : 'none')};
        position: absolute;
        left: ${(props) => props.$depth * 20 + 27}px;
        top: calc(100% - 28px);
        width: 5px;
        height: 2px;
        background-color: ${(props) => props.theme.colors.bgSurface};
        z-index: 1;
    }
`;

export const QueryGroupHeader = styled.div<{ $depth: number; $hasChildren: boolean }>`
    display: flex;
    align-items: center;
    margin: 8px 0;
    padding-left: ${(props) => props.$depth * 20 + (props.$hasChildren ? 20 : 48)}px;
    background-color: ${(props) => props.theme.colors.bgSurface};
`;

export const ExpandButton = styled.button`
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    width: 28px;
    height: 28px;
    padding: 0;
    border: 0;
    background: transparent;
    color: ${(props) => props.theme.colors.icon};
    cursor: pointer;
`;
