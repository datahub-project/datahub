import initWASM from '/assets/viz.js/render.browser.js';

initWASM({
  locateFile() {
    return '/assets/viz.js/render.wasm';
  }
});
