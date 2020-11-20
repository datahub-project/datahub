Debugging Guide for Various Errors When Trying to Run Ember Serve
======================================================================

## Node Sass Error

Sample Error:

```
Node Sass could not find a binding for your current environment: OS X 64-bit with Node.js 13.x

Found bindings for the following environments:
  - OS X 64-bit with Unsupported runtime (83)
  - OS X 64-bit with Unsupported runtime (88)

This usually happens because your environment has changed since running `npm install`.
Run `npm rebuild node-sass` to download the binding for your current environment.
```

Problem:
Your version of node currently being used is not supported by node-sass. For more information on
node-sass support, check out this link: https://www.npmjs.com/package/node-sass

Solution:
Change your version of node to the correct corresponding version, and then from `datahub-web/`
run the command

```
npm rebuild
```
