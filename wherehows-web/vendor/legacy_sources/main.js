function legacyMain() {
  var token = $("#csrfToken").val();

  // $("#settingsbtn").click(function () {
  //   var url = '/api/v1/user/me'
  //   $
  //       .get
  //       (url
  //           , function (data) {
  //             var settings = data.user.userSetting || {}
  //             for (var key in settings) {
  //               $("form[name='settingsForm'] [name='" + key + "']").val(settings[key])
  //             }
  //           }
  //       ).fail(function () {
  //     console.error("Could not pull data")
  //   })
  //   $("#settingsModal").modal('show')
  // });

  // $("#submitSettingsForm").on('click', function () {
  //   var settings = {}
  //   $("form[name='settingsForm']").serializeArray().map(function (item) {
  //     var name = item.name.replace(/([A-Z])/g, function ($1) {
  //       return "_" + $1.toLowerCase();
  //     })
  //     settings[name] = item.value
  //   })
  //   var token = $("#csrfToken").val().replace('/', '')
  //   var url = "/api/v1/usersettings/me"
  //   settings.csrfToken = token
  //   $.ajax({
  //     url: url,
  //     method: 'POST',
  //     headers: {
  //       'Csrf-Token': token
  //     },
  //     dataType: 'json',
  //     data: settings
  //   }).done(function (data, txt, xhr) {
  //     $("#settingsModal").modal('hide')
  //     location.reload()
  //   }).fail(function (xhr, txt, err) {
  //     console.log('Error: Could not update settings.')
  //   })
  // })

  var currentTab = 'Datasets';

  var width = $(window).width() * 0.99;
  var height = ($(window).height() * 0.99) - 82;

  // $('#mainSplitter').height(height).splitter({
  //   type: "v",
  //   minLeft: 100,
  //   sizeLeft: 250,
  //   minRight: 100,
  //   dock: "left",
  //   dockSpeed: 200,
  //   dockKey: 'Z',
  //   accessKey: 'I'
  // });

  var splitterResize = false;
  var skipResize = false;

  // $('#listviewbtn').click(function () {
  //   $('#listviewbtn').removeClass('btn-primary');
  //   $('#listviewbtn').removeClass('btn-default');
  //   $('#treeviewbtn').removeClass('btn-primary');
  //   $('#treeviewbtn').removeClass('btn-default');
  //   $('#listviewbtn').addClass('btn-primary');
  //   $('#treeviewbtn').addClass('btn-default');
  //   $('#tree1').hide();
  //   $('#tree2').hide();
  //   $('#tree3').hide();
  //   $('#datasetlist').show();
  //   $('#metriclist').show();
  //   $('#flowlist').show();
  // });

  // $('#treeviewbtn').click(function () {
  //   $('#listviewbtn').removeClass('btn-primary');
  //   $('#listviewbtn').removeClass('btn-default');
  //   $('#treeviewbtn').removeClass('btn-primary');
  //   $('#treeviewbtn').removeClass('btn-default');
  //   $('#listviewbtn').addClass('btn-default');
  //   $('#treeviewbtn').addClass('btn-primary');
  //   $('#datasetlist').hide();
  //   $('#metriclist').hide();
  //   $('#flowlist').hide();
  //   $('#tree1').show();
  //   $('#tree2').show();
  //   $('#tree3').show();
  // });

  // $(window).resize(function () {
  //   if (skipResize)
  //     return;
  //   if (splitterResize)
  //     clearTimeout(splitterResize);
  //   skipResize = true;
  //   splitterResize = setTimeout(function () {
  //     var height = ($(window).height() * 0.99) - 82;
  //     var width = $(window).width() * 0.99;
  //     $("#mainSplitter").css("height", height, "width", width).trigger("resize");
  //     skipResize = false;
  //   }, 500);
  // });

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