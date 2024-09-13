import { Button, Text } from '@src/alchemy-components';
import React from 'react';
import { Tooltip } from 'antd';
import { AddElementContainer, LeftSection } from './styledComponents';

interface Props {
    heading: string;
    description: string;
    buttonLabel: string;
    buttonOnClick?: () => void;
    isButtonDisabled?: boolean;
    isButtonHidden?: boolean;
    buttonTooltip?: string;
}

const AddElement = ({
    heading,
    description,
    buttonLabel,
    buttonOnClick,
    isButtonDisabled = false,
    isButtonHidden = false,
    buttonTooltip,
}: Props) => {
    return (
        <AddElementContainer>
            <LeftSection>
                <Text size="lg" weight="bold">
                    {heading}
                </Text>
                <Text size="md" color="gray">
                    {description}
                </Text>
            </LeftSection>
            {!isButtonHidden && (
                <Tooltip title={buttonTooltip}>
                    <div>
                        <Button onClick={buttonOnClick} isDisabled={isButtonDisabled}>
                            {buttonLabel}
                        </Button>
                    </div>
                </Tooltip>
            )}
        </AddElementContainer>
    );
};

export default AddElement;
