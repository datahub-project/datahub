import { hasOperationName } from "../utils";

export function setLineageV3FeatureFlags() {
  cy.intercept("POST", "/api/v2/graphql", (req) => {
    if (hasOperationName(req, "appConfig")) {
      req.alias = "gqlappConfigQuery";

      req.on("response", (res) => {
        res.body.data.appConfig.featureFlags.themeV2Enabled = true;
        res.body.data.appConfig.featureFlags.themeV2Default = true;
        res.body.data.appConfig.featureFlags.showNavBarRedesign = true;
        res.body.data.appConfig.featureFlags.lineageGraphV3 = true;
      });
    } else if (hasOperationName(req, "getMe")) {
      req.alias = "gqlgetMeQuery";
      req.on("response", (res) => {
        res.body.data.me.corpUser.settings.appearance.showThemeV2 = true;
      });
    }
  });
}
