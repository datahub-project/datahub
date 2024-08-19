import { Button, Text } from '@src/alchemy-components';
import React from 'react';
import { AddElementContainer, LeftSection } from './styledComponents';

interface Props {
    heading: string;
    description: string;
    buttonLabel: string;
    buttonOnClick?: () => void;
}

const AddElement = ({ heading, description, buttonLabel, buttonOnClick }: Props) => {
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
                <Button onClick={buttonOnClick}>{buttonLabel}</Button>
            </div>
        </AddElementContainer>
    );
};

export default AddElement;
