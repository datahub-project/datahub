function legacyMain() {
  var markedRendererOverride = new marked.Renderer()
  markedRendererOverride.link = function (href, title, text) {
    return "<a href='" + href + "' title='" + (title || text) + "' target='_blank'>" + text + "</a>";
  }

  marked.setOptions({
    gfm: true,
    tables: true,
    renderer: markedRendererOverride
  })
}
