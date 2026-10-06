import { Tooltip } from '@components';
import { Funnel } from '@phosphor-icons/react/dist/csr/Funnel';
import { Button } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const StyledButton = styled(Button)`
    && {
        margin: 0px;
        margin-left: 6px;
        padding: 0px;
    }
`;

const SaveAsViewText = styled.span`
    &&& {
        margin-left: 4px;
    }
`;

const ToolTipHeader = styled.div`
    margin-bottom: 12px;
`;

type Props = {
    onClick: () => void;
};

export const SaveAsViewButton = ({ onClick }: Props) => {
    const { t } = useTranslation('search');
    return (
        <Tooltip
            placement="right"
            title={
                <>
                    <ToolTipHeader>{t('saveAsView.tooltipHeader')}</ToolTipHeader>
                    <div>{t('saveAsView.tooltipDescription')}</div>
                </>
            }
        >
            <StyledButton type="link" onClick={onClick}>
                <Funnel size={12} />
                <SaveAsViewText>{t('saveAsView.label')}</SaveAsViewText>
            </StyledButton>
        </Tooltip>
    );
};
