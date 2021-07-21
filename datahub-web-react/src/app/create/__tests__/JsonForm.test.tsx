import React from 'react';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';
import userEvent from '@testing-library/user-event';
import { mocks } from '../../../Mocks';
import TestPageContainer from '../../../utils/test-utils/TestPageContainer';
import { JsonForm } from '../Components/JsonForm';
import JsonSchemaEditor from "@optum/json-schema-editor";

describe('JsonForm', () => {

    it('test reset button', async () => {
        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <JsonForm />
                </TestPageContainer>
            </MockedProvider>,
        );

        // input some value
        userEvent.type(screen.getByLabelText('Dataset Name'), 'json dataset');
        expect(screen.getByDisplayValue('json dataset')).toBeInTheDocument();

        const resetButton = screen.getByRole('button', { name: 'Reset' });
        fireEvent.click(resetButton);
        // should reset the value
        expect(screen.queryByDisplayValue('json dataset')).not.toBeInTheDocument();
    });

    it('test visibility of add child node button', async () => {
        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <JsonForm />
                </TestPageContainer>
            </MockedProvider>,
        );

        const select = screen.getByTestId("jsonschema-editor").children[2].children[0];
        // set option to object value
        fireEvent.change(select, { target: { value: "object" } });
        expect(screen.getByLabelText('Add Child Node')).toBeInTheDocument();

    });

});
