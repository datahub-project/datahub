declare const config: {
  environment: 'development' | 'test' | 'production';
  modulePrefix: string;
  podModulePrefix: string;
  locationType: string;
  rootURL: string;
  APP: {
    // Alternate value for notifications service toast delay, used in test runs
    notificationsTimeout?: number;
  };
};

export default config;
