import { FilterOutlined } from '@ant-design/icons';
import { Button, Tooltip } from 'antd';
import React from 'react';
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
    return (
        <Tooltip
            placement="right"
            title={
                <>
                    <ToolTipHeader>Save these filters as a new View.</ToolTipHeader>
                    <div>Views allow you to easily save or share search filters.</div>
                </>
            }
        >
            <StyledButton type="link" onClick={onClick}>
                <StyledFilterOutlined />
                <SaveAsViewText>Save as View</SaveAsViewText>
            </StyledButton>
        </Tooltip>
    );
};
