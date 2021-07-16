import React from 'react';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';
import { BrowserRouter as Router } from 'react-router-dom';
import userEvent from '@testing-library/user-event';
import { mocks } from '../../../Mocks';
import { AdHocPage } from '../AdHocPage';
import TestPageContainer from '../../../utils/test-utils/TestPageContainer';
import { CsvForm } from '../Components/CsvForm';
import { CommonFields } from '../Components/CommonFields';

describe('AdHocPage', () => {
    it('test common fields', async () => {
        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <CommonFields />
                </TestPageContainer>
            </MockedProvider>,
        );

        // dataset name
        userEvent.type(screen.getByLabelText('Dataset Name'), 'test dataset name');
        expect(screen.getByDisplayValue('test dataset name')).toBeInTheDocument();

        // dataset description
        userEvent.type(screen.getByLabelText('Dataset Description'), 'test dataset description');
        expect(screen.getByDisplayValue('test dataset name')).toBeInTheDocument();

        // dataset origin
        userEvent.type(screen.getByLabelText('Dataset Origin'), 'test dataset origin');
        expect(screen.getByDisplayValue('test dataset origin')).toBeInTheDocument();

        // dataset location
        userEvent.type(screen.getByLabelText('Dataset Location'), 'test dataset location');
        expect(screen.getByDisplayValue('test dataset location')).toBeInTheDocument();

        // console.log(prettyDOM(container));
    });

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

    it('test toggle csv/json tabs', async () => {
        render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer>
                    <Router>
                        <AdHocPage />
                    </Router>
                </TestPageContainer>
            </MockedProvider>,
        );

        const panel1 = screen.getByRole('tab', { name: 'Json' });
        const panel2 = screen.getByRole('tab', { name: 'Csv' });

        // toggle to json tab
        fireEvent.click(panel1);
        expect(screen.getByText('Click here to infer schema from json file')).toBeInTheDocument();

        // toggle to csv tab
        fireEvent.click(panel2);
        expect(
            screen.getByText('Click here to parse your file header (CSV or delimited text file only)'),
        ).toBeInTheDocument();
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
        expect(screen.getAllByText('Select field type').length).toBe(2);

        // remove one field
        const removeIcon = screen.getAllByTestId('delete-icon');
        userEvent.click(removeIcon[1]);
        // assert to 1 field
        expect(screen.getAllByText('Select field type').length).toBe(1);
    });
});
