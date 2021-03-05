const CracoAntDesignPlugin = require('craco-antd');

const themeConfig = require('./src/conf/theme/theme_light.config.json');

module.exports = {
    plugins: [
        {
            plugin: CracoAntDesignPlugin,
            options: {
                customizeTheme: themeConfig.antdStylingOverrides,
            },
        },
    ],
};
