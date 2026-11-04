import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render } from '@testing-library/react';
import React from 'react';

import { NameSourceStep } from '@app/ingestV2/source/builder/NameSourceStep';
import CustomThemeProvider from '@src/CustomThemeProvider';

const renderWithTheme = (ui: React.ReactElement) => render(<CustomThemeProvider>{ui}</CustomThemeProvider>);

describe('NameSourceStep', () => {
    // Mock IntersectionObserver
    beforeAll(() => {
        class MockIntersectionObserver {
            observe() {}

            unobserve() {}

            disconnect() {}
        }
        (window as any).IntersectionObserver = MockIntersectionObserver;
    });

    afterAll(() => {
        delete (window as any).IntersectionObserver;
    });

    it('should trim leading and trailing whitespaces from the text field on blur', () => {
        let updatedState;
        const updateStateMock = (newState) => {
            updatedState = newState;
        };
        const state = { name: '' };
        const { getByTestId } = renderWithTheme(
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
    it('should write extra env vars from the advanced section into config.extraArgs', () => {
        let updatedState;
        const updateStateMock = (newState) => {
            updatedState = newState;
        };
        const state = { name: 'source', config: { debugMode: true } };
        const { getByTestId } = renderWithTheme(
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
        const envInput = getByTestId('extra-args-input') as HTMLInputElement;
        fireEvent.change(envInput, { target: { value: '{"A": "b"}' } });

        expect(updatedState).toEqual({
            name: 'source',
            config: { debugMode: true, extraArgs: [{ key: 'extra_env_vars', value: '{"A": "b"}' }] },
        });
    });
});
