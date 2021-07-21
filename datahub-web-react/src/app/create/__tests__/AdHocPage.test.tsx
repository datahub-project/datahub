import React from 'react';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';
import { BrowserRouter as Router } from 'react-router-dom';
import userEvent from '@testing-library/user-event';
import { mocks } from '../../../Mocks';
import { AdHocPage } from '../AdHocPage';
import TestPageContainer from '../../../utils/test-utils/TestPageContainer';
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

});
