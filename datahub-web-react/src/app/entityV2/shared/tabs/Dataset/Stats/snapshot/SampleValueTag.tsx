import { Pill, Tooltip } from '@components';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

// margin-right keeps the spacing antd Tag applied between sample values
const StyledPill = styled(Pill)`
    cursor: pointer;
    max-width: 250px;
    margin-right: 8px;

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
            <StyledPill label={value} variant="squareOutline" color="gray" size="sm" clickable onPillClick={onClick} />
        </Tooltip>
    );
}
