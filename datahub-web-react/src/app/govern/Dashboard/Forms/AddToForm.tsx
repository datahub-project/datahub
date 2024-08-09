import { Text } from '@src/alchemy-components';
import { Checkbox, Divider } from 'antd';
import React from 'react';
import AddElement from './AddElement';
import { OwnershipCheckbox } from './styledComponents';

const AddToForm = () => {
    return (
        <>
            <AddElement heading="Add Questions" description="Add some questions" buttonLabel="Add Questions" />
            <Divider />
            <AddElement
                heading="Assign Assets"
                description="Assign the Assets for which you want to collect the data"
                buttonLabel="Add Assets"
            />
            <Divider />

            <AddElement
                heading="Add Recipients"
                description="Add Users and Groups to collect the data from"
                buttonLabel="Add Users"
            />
            <OwnershipCheckbox>
                <Checkbox />
                <Text size="lg" color="gray">
                    Asset owners will be prompted to fill out this form.
                </Text>
            </OwnershipCheckbox>
        </>
    );
};

export default AddToForm;
