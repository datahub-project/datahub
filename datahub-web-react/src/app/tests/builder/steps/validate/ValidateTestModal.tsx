import React from 'react';
import styled from 'styled-components';

import { EmbeddedListSearchModal } from '@app/entity/shared/components/styled/search/EmbeddedListSearchModal';
import { getValidateEntityAction } from '@app/tests/builder/steps/validate/ValidateTestButton';
import { TestBuilderState } from '@app/tests/builder/types';

const TitleContainer = styled.div`
    display: flex;
    justify-content: space-between;
`;

type Props = {
    state: TestBuilderState;
    onClose?: () => void;
};

export const ValidateTestModal = ({ state, onClose }: Props) => {
    return (
        <EmbeddedListSearchModal
            title={<TitleContainer>Try your test</TitleContainer>}
            entityAction={getValidateEntityAction(state)}
            onClose={onClose}
        />
    );
};
