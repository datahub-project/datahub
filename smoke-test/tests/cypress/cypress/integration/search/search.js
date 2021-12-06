describe('search', () => {
  it('can hit all entities search, see some results (testing this any more is tricky because it is cached for now)', () => {
    cy.login();
    cy.visit('/');
    cy.get('input[data-testid=search-input]').type('*{enter}');
	cy.contains('of 0 results').should('not.exist');
	cy.contains(/of [0-9]+ results/);
  });

  it('can hit all entities search with an impossible query and find 0 results', () => {
    cy.login();
    cy.visit('/');
    // random string that is unlikely to accidentally have a match
    cy.get('input[data-testid=search-input]').type('zzzzzzzzzzzzzqqqqqqqqqqqqqzzzzzzqzqzqzqzq{enter}');
	cy.contains('of 0 results');
  });

  it('can search, find a result, and visit the dataset page', () => {
    cy.login();
    cy.visit('http://localhost:9002/search?filter_entity=DATASET&filter_tags=urn%3Ali%3Atag%3ACypress&page=1&query=users_created')
	cy.contains('of 1 result');

	cy.contains('Cypress')

	cy.contains('fct_cypress_users_created').click();

	// platform
	cy.contains('Hive');

	// entity type
	cy.contains('Dataset');

	// entity name
	cy.contains('fct_cypress_users_created');

	// column name
	cy.contains('user_id');
	// column description
	cy.contains('Id of the user');

	// table description
	cy.contains('table containing all the users created on a single day');
  });
})