export const hasOperationName = (req, operationName) => {
  const { body } = req;
  return (
    body.hasOwnProperty("operationName") && body.operationName === operationName
  );
};

// Alias query if operationName matches
export const aliasQuery = (req, operationName) => {
  if (hasOperationName(req, operationName)) {
    req.alias = `gql${operationName}Query`;
  }
};

export const getThemeV2Interceptor = (isEnabled) => {
  return (req, res) => {
    if (hasOperationName(req, "appConfig")) {
      res.body.data.appConfig.featureFlags.themeV2Enabled = isEnabled;
      res.body.data.appConfig.featureFlags.themeV2Default = isEnabled;
      res.body.data.appConfig.featureFlags.showNavBarRedesign = isEnabled;
    }

    if (hasOperationName(req, "getMe")) {
      res.body.data.me.corpUser.settings.appearance.showThemeV2 = isEnabled;
    }
  };
};

export const applyGraphqlInterceptors = (interceptors) => {
  cy.intercept("POST", "/api/v2/graphql", (req) => {
    req.on("response", (res) => {
      interceptors.forEach((interceptor) => interceptor(req, res));
    });
  });
};
