import React from 'react';
import styled from 'styled-components';

import TestCardActionMenu from '@app/tests/card/TestCardActionMenu';
import TestCardEditButton from '@app/tests/card/TestCardEditButton';

import { Test } from '@types';

const Container = styled.div``;

type Props = {
    test: Test;
    showEdit: boolean;
    showDelete: boolean;
    onClickEdit: () => void;
    onClickDelete: () => void;
    index: number;
};

export const TestCardActions = ({ test, showEdit, showDelete, onClickEdit, onClickDelete, index }: Props) => {
    return (
        <Container>
            {showEdit && <TestCardEditButton onClickEdit={onClickEdit} index={index} />}
            {showDelete && test.urn && (
                <TestCardActionMenu onClickDelete={onClickDelete} index={index} urn={test.urn} />
            )}
        </Container>
    );
};
