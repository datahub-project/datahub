import { createLoginUsers } from '../../src/graphql-mock/fixtures/user';
import { makeServer } from '../../src/graphql-mock/server';
import { login, logout } from '../helper/authHelper';

describe('Login', () => {
    let server;

    beforeEach(() => {
        server = makeServer('test');
        createLoginUsers(server);
    });

    afterEach(() => {
        server.shutdown();
    });

    describe('given the login page is loaded', () => {
        describe('when logging in with incorrect credentials', () => {
            it('then the login should fail and the toast notification should be briefly displayed', () => {
                login('kafkaa');

                cy.contains('Failed to log in!').should('be.visible');
            });
        });

        describe('when logging in with correct credentials', () => {
            it('then the home page should be displayed', () => {
                login('kafka');

                cy.contains('Welcome back,').should('be.visible');
                cy.contains('Datasets').should('be.visible');
                cy.contains('Dashboard').should('be.visible');
                cy.contains('Chart').should('be.visible');
                cy.contains('Pipelines').should('be.visible');

                logout('kafka');
            });
        });
    });
});
