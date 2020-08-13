@datahub/pdsc
===============================================================

<strong>Original Author: Marius Seritan</strong>

This package is used to translate backend models, which are currently written in PDL (you can find
out more about PDL as part of LinkedIn's open source rest.li framework at
[https://linkedin.github.io/rest.li/pdl_schema](https://linkedin.github.io/rest.li/pdl_schema) ) and
translated to PDSC, an older schema spec for rest.li, documented at
[https://linkedin.github.io/rest.li/pdsc_syntax](https://linkedin.github.io/rest.li/pdsc_syntax).

The PDSC defines the models on the backend, which then are translated to Java classes that get
returned in our APIs. Thus, the translation of these models to TypeScript gives us a reliable method
to create a JavaScript readable equivalent definition for the API response.
