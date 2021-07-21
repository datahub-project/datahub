export const login = (username) => {
    cy.visit('/');
    cy.get('input#username').type(username);
    cy.get('input#password').type(username);
    cy.contains('Log in').click();
};

export const logout = (username) => {
    cy.get(`a[href="/user/urn:li:corpuser:${username}"]`).children('.anticon.anticon-caret-down').trigger('mouseover');
    cy.get('li#user-profile-menu-logout').click();
};
