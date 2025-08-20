import { Button, Menu } from '@components';
import React, { useCallback, useState } from 'react';
import styled from 'styled-components';

import useAssetPropertiesContext from '@app/entityV2/summary/properties/context/useAssetPropertiesContext';
import useAddPropertyMenuItems from '@app/entityV2/summary/properties/hooks/useAddPropertyMenuItems';
import { AssetProperty } from '@app/entityV2/summary/properties/types';

const StyledButton = styled(Button)`
    // prevent horizontal stretching of the button
    height: 36px;
`;

export default function AddPropertyButton() {
    const [isOpened, setIsOpened] = useState<boolean>(false);
    const { add } = useAssetPropertiesContext();

    const onAddProperty = useCallback(
        (property: AssetProperty) => {
            add(property);
            setIsOpened(false);
        },
        [add],
    );

    const menuItems = useAddPropertyMenuItems(onAddProperty);

    return (
        <Menu open={isOpened} onOpenChange={(open) => setIsOpened(open)} items={menuItems} trigger={['click']}>
            <StyledButton
                color="gray"
                variant="text"
                size="xl"
                isCircle
                icon={{ icon: 'Plus', source: 'phosphor', color: 'gray', size: '2xl' }}
            />
        </Menu>
    );
}
