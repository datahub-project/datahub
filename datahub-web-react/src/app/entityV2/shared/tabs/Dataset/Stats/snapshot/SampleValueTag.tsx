import { Tooltip } from '@components';
import { Tag } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const StyledTag = styled(Tag)`
    cursor: pointer;
    max-width: 250px;
    overflow: hidden;
    text-overflow: ellipsis;

    &&:hover {
        color: ${(props) => props.theme.colors.textHover};
        border-color: ${(props) => props.theme.colors.borderBrand};
    }
`;

type Props = {
    value: string;
};

export default function SampleValueTag({ value }: Props) {
    const { t: tf } = useTranslation('common.feedback');
    const [copied, setCopied] = useState(false);

    const onClick = () => {
        setCopied(true);
        navigator.clipboard.writeText(value);
        setTimeout(() => setCopied(false), 2000);
    };

    return (
        <Tooltip title={copied ? tf('copied') : tf('clickToCopy')}>
            <StyledTag onClick={onClick}>{value}</StyledTag>
        </Tooltip>
    );
}
