import React from 'react';
import { Tag } from 'antd';
import { CheckCircleFilled, CloseCircleFilled, ExclamationCircleFilled } from '@ant-design/icons';
import styled from 'styled-components';
import { AssertionResultType } from '../../../../../../../../../../types.generated';

const StyledTag = styled(Tag)<{ borderColor: string }>`
    &&& {
        border-color: ${(props) => props.borderColor};
        color: #373d44;
    }
`;

type Props = {
    assertionResultType: AssertionResultType;
};

const statusConfig = {
    [AssertionResultType.Success]: {
        Icon: CheckCircleFilled,
        text: 'Passed',
        colors: {
            icon: '#237804',
            border: '#B7EB8F',
            background: '#F6FFED',
        },
    },
    [AssertionResultType.Failure]: {
        Icon: CloseCircleFilled,
        text: 'Failed',
        colors: {
            icon: '#A8071A',
            border: '#FFA39E',
            background: '#FFF1F0',
        },
    },
    [AssertionResultType.Error]: {
        Icon: ExclamationCircleFilled,
        text: 'Error',
        colors: {
            icon: '#D48806',
            border: '#FFD666',
            background: '#FFFBE6',
        },
    },
};

export const AssertionStatusTag = ({ assertionResultType }: Props) => {
    const config = statusConfig[assertionResultType];
    if (!config) return null;

    const { Icon, text, colors } = config;

    return (
        <StyledTag icon={<Icon style={{ color: colors.icon }} />} color={colors.background} borderColor={colors.border}>
            {text}
        </StyledTag>
    );
};
