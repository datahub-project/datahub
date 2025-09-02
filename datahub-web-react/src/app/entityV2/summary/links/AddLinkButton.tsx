import { Button } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import AddLinkModal from '@app/entityV2/summary/links/AddLinkModal';

const StyledButton = styled(Button)`
    width: fit-content;
`;

export default function AddLinkButton() {
    const [showAddLinkModal, setShowAddLinkModal] = useState(false);

    const handleButtonClick = () => {
        setShowAddLinkModal(true);
    };
    return (
        <>
            <StyledButton variant="text" icon={{ icon: 'Plus', source: 'phosphor' }} onClick={handleButtonClick}>
                Add Link
            </StyledButton>
            {showAddLinkModal && <AddLinkModal setShowAddLinkModal={setShowAddLinkModal} />}
        </>
    );
}
