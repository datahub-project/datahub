import React from 'react';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';
import userEvent from '@testing-library/user-event';
import axios from 'axios';
import { mocks } from '../../../Mocks';
import TestPageContainer from '../../../utils/test-utils/TestPageContainer';
import { JsonForm } from '../Components/JsonForm';

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

    it('test add child node button and remove node button', async () => {
        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <JsonForm />
                </TestPageContainer>
            </MockedProvider>,
        );

        // set option to object value
        const select = screen.getByTestId('jsonschema-editor').children[2].children[0];
        fireEvent.change(select, { target: { value: 'object' } });
        expect(screen.getByLabelText('Add Child Node')).toBeInTheDocument();

        // click on add child node
        fireEvent.click(screen.getByLabelText('Add Child Node'));
        // expect "Remove Node" button to appear
        expect(screen.getByLabelText('Remove Node')).toBeInTheDocument();

        // click on remove node button
        fireEvent.click(screen.getByLabelText('Remove Node'));
        // expect "Remove Node" button to appear
        expect(screen.queryByLabelText('Remove Node')).not.toBeInTheDocument();
    });

    it('test submit button', async () => {
        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <JsonForm />
                </TestPageContainer>
            </MockedProvider>,
        );

        // create mock axios to test post request
        jest.mock('axios');
        const mockPost: jest.SpyInstance = jest.spyOn(axios, 'post');
        mockPost.mockImplementation(() =>
            Promise.resolve({
                status: 'ok',
            }),
        );

        // need to input required fields like dataset name, field_name, field_type
        userEvent.type(screen.getByLabelText('Dataset Name'), 'test dataset name');
        expect(screen.getByDisplayValue('test dataset name')).toBeInTheDocument();

        const submitButton = screen.getByRole('button', { name: 'Submit' });

        const select = screen.getByTestId('jsonschema-editor').children[2].children[0];

        // set option to object value
        fireEvent.change(select, { target: { value: 'object' } });

        userEvent.type(screen.getByPlaceholderText('Add Title'), 'test root title');
        expect(screen.getByDisplayValue('test root title')).toBeInTheDocument();

        userEvent.type(screen.getByPlaceholderText('Add Description'), 'test root description');
        expect(screen.getByDisplayValue('test root description')).toBeInTheDocument();

        // click submit to trigger form submission to backend api
        fireEvent.click(submitButton);
        await waitFor(() => expect(screen.getByText('Status:ok - Request submitted successfully')));
    });
});
