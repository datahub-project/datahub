import { PlusOutlined } from '@ant-design/icons';
import { Button, Tooltip } from '@components';
import React from 'react';

import { ADD_UNAUTHORIZED_MESSAGE } from '@app/entityV2/shared/tabs/Dataset/Queries/utils/constants';

interface Props {
    buttonLabel?: string;
    isButtonDisabled?: boolean;
    dataTestId?: string;
    onButtonClick?: () => void;
}

const AddButton = ({ buttonLabel, isButtonDisabled, dataTestId, onButtonClick }: Props) => {
    return (
        <Tooltip placement="right" title={(isButtonDisabled && ADD_UNAUTHORIZED_MESSAGE) || 'Add a highlighted query'}>
            <Button disabled={isButtonDisabled} variant="outline" onClick={onButtonClick} data-testid={dataTestId}>
                <PlusOutlined /> {buttonLabel}
            </Button>
        </Tooltip>
    );
};

export default AddButton;
