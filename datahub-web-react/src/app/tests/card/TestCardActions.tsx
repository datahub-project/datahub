import React from 'react';
import styled from 'styled-components';

import TestCardActionMenu from '@app/tests/card/TestCardActionMenu';
import TestToggleEnabled from '@app/tests/card/TestToggleEnabled';

import { Test } from '@types';

const Container = styled.div`
    display: flex;
    align-items: center;
`;

type Props = {
    test: Test;
    onClickEdit: () => void;
    onClickDelete: () => void;
    index: number;
};

export const TestCardActions = ({ test, onClickEdit, onClickDelete, index }: Props) => {
    return (
        <Container>
            <TestToggleEnabled test={test} />
            {test.urn && (
                <TestCardActionMenu
                    onClickDelete={onClickDelete}
                    onClickEdit={onClickEdit}
                    index={index}
                    urn={test.urn}
                />
            )}
        </Container>
    );
};
