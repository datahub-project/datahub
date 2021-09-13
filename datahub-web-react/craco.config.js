require('dotenv').config();
const CracoAntDesignPlugin = require('craco-antd');
const path = require('path');

const themeConfig = require(`./src/conf/theme/${process.env.REACT_APP_THEME_CONFIG}`);

function addLessPrefixToKeys(styles) {
    const output = {};
    Object.keys(styles).forEach((key) => {
        output[`@${key}`] = styles[key];
    });
    return output;
}

module.exports = {
    plugins: [
        {
            plugin: CracoAntDesignPlugin,
            options: {
                customizeThemeLessPath: path.join(__dirname, 'src/conf/theme/global-variables.less'),
                customizeTheme: addLessPrefixToKeys(themeConfig.styles),
            },
        },
    ],
};
