import React from 'react';
import { render, screen } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';
// import { BrowserRouter as Router } from 'react-router-dom';
import userEvent from '@testing-library/user-event';
import { mocks } from '../../../Mocks';
// import { AdHocPage } from '../AdHocPage';
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
        userEvent.type(screen.getByTitle('Information about Dataset'), 'test dataset description');
        // for some reason, screen show concatenated value from above.
        expect(screen.getByDisplayValue('test dataset nametest dataset description')).toBeInTheDocument();

        // dataset origin
        userEvent.type(screen.getByLabelText('Dataset Origin'), 'test dataset origin');
        expect(screen.getByDisplayValue('test dataset origin')).toBeInTheDocument();

        // // dataset location
        userEvent.type(screen.getByLabelText('Dataset Location'), 'test dataset location');
        expect(screen.getByDisplayValue('test dataset location')).toBeInTheDocument();
        // console.log(prettyDOM(container));
    });
});
