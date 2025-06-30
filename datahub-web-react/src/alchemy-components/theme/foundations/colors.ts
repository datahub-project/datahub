const colors = {
    transparent: 'transparent',
    current: 'currentColor',
    white: '#FFFFFF',
    black: '#000000', // do not use black, use grays 600 for primary text and 1700 for secondary

    gray: {
        0: '#FFFFFF', // white
        100: '#EBECF0', // border
        200: '#CFD1DA', // should be deprecated
        300: '#A9ADBD', // should be deprecated
        400: '#81879F', // should be deprecated
        500: '#5B6282', // should be deprecated
        600: '#374066', // primary text
        700: '#2F3657', // should be deprecated
        800: '#272D48', // should be deprecated
        900: '#231A58', // should be deprecated
        1000: '#F1F3FD', // violet 0
        1100: '#F1FBFE', // blue 0
        1200: '#FBF3EF', // red 0
        1300: '#F7FBF4', // green 0
        1400: '#E9EAEE', // border
        1500: '#F9FAFC', // bg surface
        1600: '#F5F6FA', // bg of new nav
        1700: '#5F6685', // secondary text
        1800: '#8088A3', // tertiary text, disabled text, placeholder text, icon
        1900: '#A3A7B9',
        2000: '#1E2338', // dark bg-suface
        2100: '#171B2B', // dark bg
    },

    violet: {
        0: '#F1F3FD',
        100: '#CAC3F1',
        200: '#B0A7EA',
        300: '#8C7EE0',
        400: '#7565DA',
        500: '#533FD1',
        600: '#4C39BE', // primary value
        700: '#3B2D94',
        800: '#2E2373',
        900: '#231A58',
        1000: '#E5E2F8',
        1100: '#E5E2F8',
    },

    // by default, our primary pallette is the same as our violet pallette
    primary: {
        0: '#F1F3FD',
        100: '#CAC3F1',
        200: '#B0A7EA',
        300: '#8C7EE0',
        400: '#7565DA',
        500: '#533FD1', // primary value
        600: '#4C39BE',
        700: '#3B2D94',
        800: '#2E2373',
        900: '#231A58',
        1000: '#E5E2F8',
        1100: '#E5E2F8',
    },

    green: {
        0: '#F1F8EE',
        100: '#D5E9C9',
        200: '#C0DEAF',
        300: '#A4CF8A',
        400: '#92C573',
        500: '#77B750', // primary value
        600: '#6CA749',
        700: '#548239',
        800: '#41652C',
        900: '#324D22',
        1000: '#0D7543',
        1100: '#E1F0D6',
        1200: '#248F5B',
    },

    red: {
        0: '#FBF3EF',
        100: '#F6D5D5',
        200: '#F2C1C1',
        300: '#ECA5A5',
        400: '#E99393',
        500: '#CD0D24', // primary value
        600: '#CF6D6D',
        700: '#A15555',
        800: '#7D4242',
        900: '#5F3232',
        1000: '#C4360B',
        1100: '#F3DACE',
        1200: '#E54D1F',
    },

    blue: {
        0: '#F1FBFE',
        100: '#EFF8FC',
        200: '#E6F5FB',
        300: '#CCEBF6',
        400: '#5ABDE1', // primary value
        500: '#51AACB',
        600: '#4897B4',
        700: '#448EA9',
        800: '#367187',
        900: '#285565',
        1000: '#09739A',
        1100: '#CCEBF6',
    },

    yellow: {
        0: '#FFFAEB',
        100: '#FCEDC7',
        200: '#FAE4AB',
        300: '#F8D785',
        400: '#F6D06D',
        500: '#EEAE09', // primary value
        600: '#DEB242',
        700: '#AD8B34',
        800: '#866C28',
        900: '#66521F',
        1000: '#C77100',
        1100: '#FCEDC7',
        1200: '#FFFAEB',
    },
};

export default colors;
