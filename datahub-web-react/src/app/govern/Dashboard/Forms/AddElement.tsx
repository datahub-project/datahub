import { Tooltip } from '@components';
import React from 'react';

import { AddElementContainer, LeftSection } from '@app/govern/Dashboard/Forms/styledComponents';
import { Button, Text } from '@src/alchemy-components';

interface Props {
    heading: string;
    description: string;
    buttonLabel: string;
    buttonOnClick?: () => void;
    isButtonDisabled?: boolean;
    isButtonHidden?: boolean;
    buttonTooltip?: string;
    dataTestIdPrefix?: string;
}

const AddElement = ({
    heading,
    description,
    buttonLabel,
    buttonOnClick,
    isButtonDisabled = false,
    isButtonHidden = false,
    buttonTooltip,
    dataTestIdPrefix,
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
                <Tooltip title={buttonTooltip} showArrow={false}>
                    <div>
                        <Button
                            onClick={buttonOnClick}
                            disabled={isButtonDisabled}
                            data-testid={dataTestIdPrefix && `${dataTestIdPrefix}-button`}
                        >
                            {buttonLabel}
                        </Button>
                    </div>
                </Tooltip>
            )}
        </AddElementContainer>
    );
};

export default AddElement;
