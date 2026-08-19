import { Typography } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const ELLIPSIS = '...';

type Props = {
    description: any;
};

const DescriptionContainer = styled.div`
    position: relative;
    display: flex;
    flex-direction: column;
    width: 500px;
    height: 100%;
    min-height: 22px;
`;

export default function AccessManagerDescription({ description }: Props) {
    const { t } = useTranslation('common.actions');
    const shouldTruncateDescription = description.length > 150;
    const [expanded, setIsExpanded] = useState(!shouldTruncateDescription);
    const finalDescription = expanded ? description : description.slice(0, 150);
    const toggleExpanded = () => {
        setIsExpanded(!expanded);
    };

    return (
        <DescriptionContainer>
            {finalDescription}
            <Typography.Link
                onClick={() => {
                    toggleExpanded();
                }}
            >
                {(shouldTruncateDescription &&
                    (expanded ? (
                        <>&nbsp;{t('readLess')}</>
                    ) : (
                        <>
                            {ELLIPSIS}
                            {t('readMore')}
                        </>
                    ))) ||
                    undefined}
            </Typography.Link>
        </DescriptionContainer>
    );
}
