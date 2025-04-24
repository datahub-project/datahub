export interface GraphQLRequest {
  body: {
    operationName?: string;
    [key: string]: any;
  };
  alias?: string;
  on(event: string, callback: (response: any) => void): void;
}

export interface GraphQLResponse {
  body: {
    data: {
      appConfig?: {
        featureFlags: {
          themeV2Enabled: boolean;
          themeV2Default: boolean;
          showNavBarRedesign: boolean;
        };
      };
      me?: {
        corpUser: {
          settings: {
            appearance: {
              showThemeV2: boolean;
            };
          };
        };
      };
    };
  };
}
