---
description: Learn how to upload and use the DataHub Cloud Chrome extension (beta) locally before it's available on the Chrome store.
---

# DataHub Cloud Chrome Extension

## Installing the Extension

In order to use the DataHub Cloud Chrome extension, you need to download it onto your browser from the Chrome web store [here](https://chrome.google.com/webstore/detail/datahub-chrome-extension/aoenebhmfokhglijmoacfjcnebdpchfj).


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/chrome-store-extension-screenshot.png"/>
</p>


Simply click "Add to Chrome" then "Add extension" on the ensuing popup.

## Configuring the Extension

Once you have your extension installed, you'll need to configure it to work with your DataHub Cloud deployment.

1. Click the extension button on the right of your browser's address bar to view all of your installed extensions. Click on the newly installed DataHub extension.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/extension_open_popup.png"/>
</p>


2. Fill in your DataHub domain and click "Continue" in the extension popup that appears.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/extension_enter_domain.png"/>
</p>


If your organization uses standard SaaS domains for Looker, you should be ready to go!

### Additional Configurations

Some organizations have custom SaaS domains for Looker and some DataHub Cloud deployments utilize **Platform Instances** and set custom **Environments** when creating DataHub assets. If any of these situations applies to you, please follow the next few steps to finish configuring your extension.

1. Click on the extension button and select your DataHub extension to open the popup again. Now click the settings icon in order to open the configurations page.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/extension_open_options_page.png"/>
</p>


2. Fill out any and save custom configurations you have in the **TOOL CONFIGURATIONS** section. Here you can configure a custom domain, a Platform Instance associated with that domain, and the Environment set on your DataHub assets. If you don't have a custom domain but do have a custom Platform Instance or Environment, feel free to leave the field domain empty.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/extension_custom_configs.png"/>
</p>


## Using the Extension

Once you have everything configured on your extension, it's time to use it!

1. First ensure that you are logged in to your DataHub Cloud instance.

2. Navigate to Looker or Tableau and log in to view your data assets.

3. Navigate to a page where DataHub can provide insights on your data assets (Dashboards and Explores).

4. Click the DataHub Cloud extension button on the bottom right of your page to open a drawer where you can now see additional information about this asset right from your DataHub instance.


<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/extension_view_in_looker.png"/>
</p>


## Advanced: Self-Hosted DataHub

If you are using the DataHub Cloud Chrome extension for your self-hosted DataHub instance, everything above is applicable. However, there is one additional step you must take in order to set up your instance to be compatible with the extension.

### Configure Auth Cookies

In order for the Chrome extension to work with your instance, it needs to be able to make authenticated requests. Therefore, authentication cookies need to be set up so that they can be shared with the extension on your browser. You must update the values of two environment variables in your `datahub-frontend` container:

```
AUTH_COOKIE_SAME_SITE="NONE"
AUTH_COOKIE_SECURE=true
```

Once your re-deploy your `datahub-frontend` container with these values, you should be good to go!