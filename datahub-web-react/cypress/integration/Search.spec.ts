import { createLoginUsers } from '../../src/graphql-mock/fixtures/user';
import { makeServer } from '../../src/graphql-mock/server';
import { login, logout } from '../helper/authHelper';

describe('Search', () => {
    let server;

    beforeEach(() => {
        server = makeServer('test');
        createLoginUsers(server);
    });

    afterEach(() => {
        server.shutdown();
    });

    describe('given the home page is loaded', () => {
        describe('when the user enters a keyword in the search field and results found and the first item is selected from the search result dropdown', () => {
            it('then the search result page should be displayed with the Task tab be selected and the selected item be displayed', () => {
                login('kafka');

                cy.get('input[placeholder="Search Datasets, People, & more..."]').type('load');

                cy.get('div.rc-virtual-list-holder-inner')
                    .children('div.ant-select-item.ant-select-item-option.ant-select-item-option-grouped')
                    .contains('load_all_')
                    .click();

                cy.get('.ant-tabs-tab.ant-tabs-tab-active').contains('Task').should('be.visible');
                cy.contains('load_all_').should('be.visible');

                logout('kafka');
            });
        });
    });
});
