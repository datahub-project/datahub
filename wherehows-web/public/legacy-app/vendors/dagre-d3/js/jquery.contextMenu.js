/**
 * jQuery.contextMenu - Show a custom context when right clicking something
 * Jonas Arnklint, http://github.com/arnklint/jquery-contextMenu
 * Released into the public domain
 * Date: Jan 14, 2011
 * @author Jonas Arnklint
 * @version 1.7
 *
*/
// Making a local '$' alias of jQuery to support jQuery.noConflict
(function($) {
  jQuery.fn.contextMenu = function ( name, actions, options ) {
    var me = this,
    win = $(window);

    // fix for ie mouse button bug
    var mouseEvent = 'contextmenu';
    

    
    var mouseEventFunc = function(e){
      var name = e.data.name;
      var actions = e.data.actions;
      var options = e.data.options;
      var correctButton = ( (options.leftClick && e.button == 0) || (!options.leftClick && e.button == 2) );

      if( correctButton ){
        activeElement = $(this); // set clicked element

        if (options.showMenu) {
          options.showMenu.call(menu, activeElement);
        }

        var menu = $('<ul id="'+name+'" class="context-menu"></ul>').hide().appendTo('body'),
        activeElement = null, // last clicked element that responds with contextMenu
        hideMenu = function() {
          $('.context-menu:visible').each(function() {
            $(this).trigger("closed");
            $(this).hide();
            $('body').unbind('click', hideMenu);
          });

          if (options.hideMenu) {
              options.hideMenu.call(menu, activeElement);
          }
        },
        default_options = {
          disable_native_context_menu: false, // disables the native contextmenu everywhere you click
          leftClick: false // show menu on left mouse click instead of right
        },
        options = $.extend(default_options, options);

        $(document).bind('contextmenu', function(e) {
          if (options.disable_native_context_menu) {
            e.preventDefault();
          }
          hideMenu();
        });

        $.each(actions, function(me, itemOptions) {
          if (itemOptions.link) {
            var link = itemOptions.link;
          } else {
            var link = '<a href="#">'+me+'</a>';
          }

          var menuItem = $('<li>' + link + '</li>');

          if (itemOptions.klass) {
            menuItem.attr("class", itemOptions.klass);
          }

          menuItem.appendTo(menu).bind('click', function(e) {
            d3.event = e;
            itemOptions.click(activeElement);
            e.preventDefault();
          }).bind('mouseover', function(e) {
            d3.event = e;
            itemOptions.mouseover(activeElement);
            e.preventDefault();
          }).bind('mouseout', function(e) {
            d3.event = e;
            itemOptions.mouseout(activeElement);
            e.preventDefault();
          });
        });

        // Hide any existing context menus
        hideMenu();

        menu.css({
          visibility: 'hidden',
          position: 'absolute',
          zIndex: 1000
        });

        // include margin so it can be used to offset from page border.
        var mWidth = menu.outerWidth(true),
          mHeight = menu.outerHeight(true),
          xPos = ((e.pageX - win.scrollLeft()) + mWidth < win.width()) ? e.pageX : e.pageX - mWidth,
          yPos = ((e.pageY - win.scrollTop()) + mHeight < win.height()) ? e.pageY : e.pageY - mHeight;

        menu.show(0, function() {
          $('body').bind('click', hideMenu);
        }).css({
          visibility: 'visible',
          top: yPos + 'px',
          left: xPos + 'px',
          zIndex: 1000
        });

        return false;
      }
    }

    return me.bind(mouseEvent, { name: name, actions: actions, options: options}, mouseEventFunc);
  }
})(jQuery);



