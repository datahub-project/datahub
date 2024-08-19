import { FilterOutlined } from '@ant-design/icons';
import { Button, Tooltip } from 'antd';
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

const StyledFilterOutlined = styled(FilterOutlined)`
    && {
        font-size: 12px;
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
    const { t } = useTranslation();
    return (
        <Tooltip
            placement="right"
            title={
                <>
                    <ToolTipHeader>{t('filter.view.saveTheseFiltersAsANewView')}</ToolTipHeader>
                    <div>{t('filter.view.viewsAllowYouToEasilySaveOrShareSearchFilters')}</div>
                </>
            }
        >
            <StyledButton type="link" onClick={onClick}>
                <StyledFilterOutlined />
                <SaveAsViewText>{t('views.saveAsView')}</SaveAsViewText>
            </StyledButton>
        </Tooltip>
    );
};
