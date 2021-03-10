require('dotenv').config();
const CracoAntDesignPlugin = require('craco-antd');

const themeConfig = require(`./src/conf/theme/${process.env.REACT_APP_THEME_CONFIG}`);

module.exports = {
    plugins: [
        {
            plugin: CracoAntDesignPlugin,
            options: {
                customizeTheme: themeConfig.styles,
            },
        },
    ],
};
