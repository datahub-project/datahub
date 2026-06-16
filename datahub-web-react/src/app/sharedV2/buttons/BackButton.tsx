import { Tooltip } from '@components';
import KeyboardBackspaceIcon from '@mui/icons-material/KeyboardBackspace';
import { Button } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const StyledButton = styled(Button)`
    height: 25px;
    width: 25px;
    color: ${(p) => p.theme.colors.iconBrand};
    padding: 0px;
    border-radius: 20px;
    border: 1px solid ${(p) => p.theme.colors.borderBrand};
    display: flex;
    align-items: center;
    justify-content: center;
    margin-left: -4px;
    margin-right: 10px;
    margin-top: 2px;

    &:hover {
        color: ${(p) => p.theme.colors.iconBrand} !important;
        border-color: ${(p) => p.theme.colors.borderBrand} !important;
    }
`;

const StyledLeftOutlined = styled(KeyboardBackspaceIcon)`
    && {
        font-size: 20px;
        margin: 0px;
        padding 0px;
    }
`;

interface Props {
    onGoBack?: () => void;
}

export const BackButton = ({ onGoBack }: Props) => {
    const { t } = useTranslation('shared.misc');
    return (
        <Tooltip title={t('backButton.tooltip')} showArrow={false} placement="bottom">
            <StyledButton onClick={onGoBack}>
                <StyledLeftOutlined />
            </StyledButton>
        </Tooltip>
    );
};
