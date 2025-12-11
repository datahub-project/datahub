/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render } from '@testing-library/react';
import React from 'react';

import { NameSourceStep } from '@app/ingest/source/builder/NameSourceStep';

describe('NameSourceStep', () => {
    it('should trim leading and trailing whitespaces from the text field on blur', () => {
        let updatedState;
        const updateStateMock = (newState) => {
            updatedState = newState;
        };
        const state = { name: '' };
        const { getByTestId } = render(
            <MockedProvider mocks={[]} addTypename={false}>
                <NameSourceStep
                    state={state}
                    updateState={updateStateMock}
                    prev={() => {}}
                    submit={() => {}}
                    goTo={() => {}}
                    cancel={() => {}}
                    ingestionSources={[]}
                    isEditing
                />
            </MockedProvider>,
        );
        const nameInput = getByTestId('source-name-input') as HTMLInputElement;
        const SourceName = '   Test Name   ';
        nameInput.value = SourceName;
        fireEvent.change(nameInput, { target: { value: SourceName } });
        fireEvent.blur(nameInput);

        expect(updatedState).toEqual({ name: 'Test Name' });
    });
});
