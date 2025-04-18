import React from 'react';
import styled from 'styled-components';
import { EmbeddedListSearchModal } from '../../../../entity/shared/components/styled/search/EmbeddedListSearchModal';
import { TestBuilderState } from '../../types';
import { getValidateEntityAction } from './ValidateTestButton';

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
