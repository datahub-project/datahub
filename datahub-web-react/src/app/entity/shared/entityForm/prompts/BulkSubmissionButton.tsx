import React from 'react';
import styled from 'styled-components';

import { useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import { WhiteButton } from '@app/shared/components';
import { pluralize } from '@app/shared/textUtil';

const StyledButton = styled(WhiteButton)`
    align-self: end;
    margin-left: 8px;
`;

interface Props {
    isDisabled: boolean;
    submitResponse: () => void;
}

export default function BulkSubmissionButton({ isDisabled, submitResponse }: Props) {
    const {
        entity: { selectedEntities, areAllEntitiesSelected },
        search: { results },
    } = useEntityFormContext();
    const totalResults = results.searchAcrossEntities?.total || 0;

    return (
        <StyledButton disabled={isDisabled} onClick={submitResponse}>
            {areAllEntitiesSelected ? (
                <>
                    Set for {totalResults} Selected {pluralize(totalResults, 'Asset')}
                </>
            ) : (
                <>
                    Set for {selectedEntities.length} Selected {pluralize(selectedEntities.length, 'Asset')}
                </>
            )}
        </StyledButton>
    );
}
