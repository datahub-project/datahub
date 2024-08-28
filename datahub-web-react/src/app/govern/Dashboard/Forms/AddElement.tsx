import { Button, Text } from '@src/alchemy-components';
import React from 'react';
import { AddElementContainer, LeftSection } from './styledComponents';

interface Props {
    heading: string;
    description: string;
    buttonLabel: string;
    buttonOnClick?: () => void;
    isButtonDisabled?: boolean;
}

const AddElement = ({ heading, description, buttonLabel, buttonOnClick, isButtonDisabled = false }: Props) => {
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
            <div>
                <Button onClick={buttonOnClick} isDisabled={isButtonDisabled}>
                    {buttonLabel}
                </Button>
            </div>
        </AddElementContainer>
    );
};

export default AddElement;
