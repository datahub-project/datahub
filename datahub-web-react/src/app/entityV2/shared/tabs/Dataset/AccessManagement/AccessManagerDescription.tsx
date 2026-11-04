import { Button } from '@components';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

type Props = {
    description: any;
};

// A link-styled button: the toggle has no destination. align-self keeps the click target to the label.
const ToggleButton = styled(Button)`
    align-self: flex-start;
    padding: 0;
`;

const DescriptionContainer = styled.div`
    position: relative;
    display: flex;
    flex-direction: column;
    width: 500px;
    height: 100%;
    min-height: 22px;
`;

export default function AccessManagerDescription({ description }: Props) {
    const { t: tc } = useTranslation('common.actions');
    const shouldTruncateDescription = description.length > 150;
    const [expanded, setIsExpanded] = useState(!shouldTruncateDescription);
    const finalDescription = expanded ? description : description.slice(0, 150);
    const toggleExpanded = () => {
        setIsExpanded(!expanded);
    };

    return (
        <DescriptionContainer>
            {finalDescription}
            {shouldTruncateDescription && (
                <ToggleButton
                    variant="link"
                    onClick={() => {
                        toggleExpanded();
                    }}
                >
                    {expanded ? ` ${tc('readLess')}` : `...${tc('readMore')}`}
                </ToggleButton>
            )}
        </DescriptionContainer>
    );
}
