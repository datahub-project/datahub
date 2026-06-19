import { Button } from 'antd';
import React, { CSSProperties } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

const STYLE_NO_SHADOW: CSSProperties = { boxShadow: 'none' };

const StyledButton = styled(Button)`
    width: 100%;
    text-align: center;
    background-color: ${(props) => props.theme.styles['primary-color']};
    color: ${(props) => props.theme.colors.textOnFillDefault};
    border-radius: 0;
`;

const DropdownMenu = styled.div`
    background-color: ${(props) => props.theme.colors.bgSurface};
    border-radius: 5px;
    box-shadow: ${(props) => props.theme.colors.shadowMd};
    overflow: hidden;
    min-width: 200px;

    .ant-dropdown-menu-title-content {
        background-color: ${(props) => props.theme.colors.bgSurface};
        &:hover {
            background-color: ${(props) => props.theme.colors.bgSurface};
        }
    }
`;

const ScrollableContent = styled.div`
    max-height: 312px;
    overflow: auto;
`;

interface Props {
    menuOption: React.ReactNode;
    onUpdate: () => void;
    style?: CSSProperties;
}

export default function BooleanMoreFilterMenu({ menuOption, onUpdate, style }: Props) {
    const { t: tc } = useTranslation('common.actions');
    return (
        <DropdownMenu data-testid="boolean-filter-dropdown" style={style}>
            <ScrollableContent>
                {React.cloneElement(menuOption as React.ReactElement, { style: STYLE_NO_SHADOW })}
            </ScrollableContent>
            <StyledButton type="text" onClick={onUpdate} data-testid="boolean-update-filters">
                {tc('update')}
            </StyledButton>
        </DropdownMenu>
    );
}
