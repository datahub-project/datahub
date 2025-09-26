import { Button, Menu } from '@components';
import React, { useCallback, useState } from 'react';
import styled from 'styled-components';

import useAddPropertyMenuItems from '@app/entityV2/summary/properties/menuAddProperty/hooks/useAddPropertyMenuItems';
import { AssetProperty } from '@app/entityV2/summary/properties/types';
import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';

const StyledButton = styled(Button)`
    // prevent horizontal stretching of the button
    height: 36px;
`;

export default function AddPropertyButton() {
    const [isOpened, setIsOpened] = useState<boolean>(false);
    const { addSummaryElement } = usePageTemplateContext();

    const onAddProperty = useCallback(
        (property: AssetProperty) => {
            addSummaryElement({ elementType: property.type, structuredProperty: property.structuredProperty });
            setIsOpened(false);
        },
        [addSummaryElement],
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
                data-testid="add-property-button"
            />
        </Menu>
    );
}
