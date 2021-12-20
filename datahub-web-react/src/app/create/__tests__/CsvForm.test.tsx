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
            expect(screen.getByText('Error: Network Error')),
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
        const listpath = screen.getAllByPlaceholderText('browsing path');
        userEvent.type(listpath[0], 'invalid');
        await waitFor(() =>
            expect(screen.getByText('The path must start and end with a / char', { exact: false })).toBeInTheDocument(),
        );
        // add second path
        fireEvent.click(screen.getByText('Add more browsing paths'));
        const svg = screen.getAllByLabelText('minus-circle');
        // remove second path
        fireEvent.click(svg[1]);
        // there should not be a option to remove path anymore.
        expect(screen.queryAllByLabelText('minus-circle')).toHaveLength(0);
        fireEvent.click(screen.getByText('Add more browsing paths'));
        fireEvent.click(screen.getByText('Add more browsing paths'));
        fireEvent.click(screen.getByText('Add more browsing paths'));
        // warning for 4 or more paths pops up
        await waitFor(() => expect(screen.getByText('Limited to 3 Browse Paths or less!')).toBeInTheDocument());
        // remove a path, expect no warning
        fireEvent.click(screen.getAllByLabelText('minus-circle')[1]);
        await waitFor(() => expect(screen.queryByText('Limited to 3 Browse Paths or less!')).toBeInTheDocument());
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
