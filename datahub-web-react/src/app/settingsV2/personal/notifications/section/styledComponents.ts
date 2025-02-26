import styled from 'styled-components/macro';
import { Button, colors } from '@components';
import { Typography, Input, Form } from 'antd';

export const SinkConfigurationContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

export const SinkTitle = styled.div`
    font-size: 14px;
    color: ${colors.gray['600']};
    font-weight: bold;
`;

export const SinkDescription = styled(Typography.Text)`
    font-size: 14px;
    color: ${colors.gray[1700]};
`;

export const SinkButtonsContainer = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: flex-start;
    align-items: start;
    gap: 2px;
`;

export const StyledFormItem = styled(Form.Item)`
    margin-bottom: 0px;
`;

export const StyledInput = styled(Input)`
    width: 220px;
`;

export const SaveButton = styled(Button)`
    margin: 0 4px;
`;

export const CancelButton = styled(Button)``;
