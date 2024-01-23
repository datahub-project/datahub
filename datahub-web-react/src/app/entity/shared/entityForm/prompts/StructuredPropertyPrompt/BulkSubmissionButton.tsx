import React from 'react';
import styled from 'styled-components';
import { useEntityFormContext } from '../../EntityFormContext';
import { WhiteButton } from '../../../../../shared/components';
import { pluralize } from '../../../../../shared/textUtil';

const StyledButton = styled(WhiteButton)`
    align-self: end;
    margin-left: 8px;
`;

interface Props {
    isDisabled: boolean;
    submitResponse: () => void;
}

export default function BulkSubmissionButton({ isDisabled, submitResponse }: Props) {
    const { selectedEntities } = useEntityFormContext();

    if (!selectedEntities.length) return null;

    return (
        <StyledButton disabled={isDisabled} onClick={submitResponse}>
            Set for {selectedEntities.length} Selected {pluralize(selectedEntities.length, 'Asset')}
        </StyledButton>
    );
}
