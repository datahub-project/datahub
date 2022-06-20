import React from 'react';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';
import userEvent from '@testing-library/user-event';
import { mocks } from '../../../Mocks';
import TestPageContainer from '../../../utils/test-utils/TestPageContainer';
import { CsvForm } from '../Components/CsvForm';

describe('CsvForm', () => {
    it('test reset button', async () => {
        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <CsvForm />
                </TestPageContainer>
            </MockedProvider>,
        );

        // input some value
        expect(screen.getByLabelText('Dataset Name')).toBeInTheDocument();
        userEvent.type(screen.getByLabelText('Dataset Name'), 'test dataset name');
        expect(screen.getByDisplayValue('test dataset name')).toBeInTheDocument();

        const resetButton = screen.getByRole('button', { name: 'Reset' });
        fireEvent.click(resetButton);
        // should reset the value
        expect(screen.queryByDisplayValue('test dataset name')).not.toBeInTheDocument();
    });

    it('test submit button - error response', async () => {
        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <CsvForm />
                </TestPageContainer>
            </MockedProvider>,
        );

        // need to input required fields like dataset name, field_name, field_type
        userEvent.type(screen.getByLabelText('Dataset Name'), 'test dataset name');
        expect(screen.getByDisplayValue('test dataset name')).toBeInTheDocument();

        const submitButton = screen.getByRole('button', { name: 'Submit' });
        userEvent.type(screen.getByPlaceholderText('Field Name'), 'fieldA');

        // handle select action
        const select = screen.getAllByRole('combobox');
        select.filter((item) => {
            if (item.id === 'dynamic_form_item_fields_0_field_type') {
                fireEvent.mouseDown(item);
                fireEvent.click(screen.getByText('Number'));
                return true;
            }
            return false;
        });

        // need to key in select option for field type to pass validation rule
        fireEvent.click(submitButton);
        await waitFor(() =>
            // return error since the api endpoint is not up
            expect(screen.getByText('Confirm Dataset Name: test dataset name?', { exact: false })),
        );
    });

    it('test browsepath component', async () => {
        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <CsvForm />
                </TestPageContainer>
            </MockedProvider>,
        );
        // click on the existing /json/ browsepath and append a word to it, making it invalid
        expect(await screen.getAllByPlaceholderText('browsing path').length).toBe(1);
        const lists = screen.getAllByPlaceholderText('browsing path');
        userEvent.type(lists[0], 'invalid');
        //     // // add second path
        fireEvent.click(screen.getByText('Add more browsing paths'));
        expect(await screen.getAllByPlaceholderText('browsing path').length).toBe(2);
        fireEvent.click(screen.getByText('Add more browsing paths'));
        expect(await screen.getAllByPlaceholderText('browsing path').length).toBe(3);
        await waitFor(() =>
            expect(screen.getByText('The path must start and end with a / char', { exact: false })).toBeInTheDocument(),
        );
    });

    it('test add and remove field button', async () => {
        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <CsvForm />
                </TestPageContainer>
            </MockedProvider>,
        );

        const addFieldButton = screen.getByText('Add field');

        // click the button to add new field
        fireEvent.click(addFieldButton);
        // assert to 2 fields
        expect(screen.getAllByText('String').length).toBe(2);

        // remove one field
        const removeIcon = screen.getAllByTestId('delete-icon');
        userEvent.click(removeIcon[1]);
        // assert to 1 field
        expect(screen.getAllByText('String').length).toBe(1);
    });
});
