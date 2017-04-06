var toOuterHTML = function(data) {
	return jQuery('<div />').append(data.eq(0).clone()).html();
};

var timestampToTimeString = function(timestamp) {
	timestamp = Math.floor(timestamp);
	var date = new Date(timestamp);
	var hours = date.getHours();
	var minutes = date.getMinutes();
	minutes = minutes < 10 ? '0'+minutes : minutes;
	var seconds = date.getSeconds();
	seconds = seconds < 10 ? '0'+seconds : seconds;
	var milliseconds = date.getMilliseconds();
	milliseconds = milliseconds < 10 ? '00'+milliseconds : milliseconds < 100 ? '0'+milliseconds : milliseconds;
	return hours + ":" + minutes + ":" + seconds + "." + milliseconds;
}

var d3LineageTooltip = function(gravity) {

	var tp = Tooltip(gravity).title(function(d) {
        if (!(g_currentData && g_currentData.nodes))
          return '';

      var data;

      data = g_currentData.nodes[d];
      if (!data)
        return '';
      var t = $("<div>").attr("class", "xtrace-tooltip");

      if (data.hasOwnProperty('_sort_list')){
        for(k in data['_sort_list']){
          var key = data['_sort_list'][k];
          appendRow(key, data[key], t);
        }
      }else{
        $.each(data, function(d) {
          //appendRow(d.key, data[d].join(", "), tooltip);
          appendRow(d, data[d], t);
        });
      }
		//var reserved = ["Source", "Operation", "Agent", "Label", "Class", "Timestamp", "HRT", "Cycles", "Host", "ProcessID", "ThreadID", "ThreadName", "X-Trace"];

	  function appendRow(key, value, tooltip) {
	    var keyrow = $("<div>").attr("class", "key").append(key);
	    var valrow = $("<div>").attr("class", "value").append(value);
	    var clearrow = $("<div>").attr("class", "clear");
	    tooltip.append($("<div>").append(keyrow).append(valrow).append(clearrow));
	  }
      //return tooltip.outerHTML();
      return toOuterHTML(t);
	  });
    return tp;
}

var CompareTooltip = function() {

	var tooltip = Tooltip().title(function(d) {
		function appendRow(key, value, tooltip) {
			var keyrow = $("<div>").attr("class", "key").append(key);
			var valrow = $("<div>").attr("class", "value").append(value);
			var clearrow = $("<div>").attr("class", "clear");
			tooltip.append($("<div>").append(keyrow).append(valrow).append(clearrow));
		}

		var tooltip = $("<div>").attr("class", "xtrace-tooltip");

		appendRow("ID", d.get_id(), tooltip);
		appendRow("NumReports", d.get_node_ids().length, tooltip);
		appendRow("NumLabels", Object.keys(d.get_labels()).length, tooltip);

		return tooltip.outerHTML();
	});

	return tooltip;

}


var Tooltip = function(gravity) {
	if (gravity==null)
		gravity = $.fn.tipsy.autoWE;

	var tooltip = function(selection) {
		selection.each(function(d) {
			$(this).tipsy({
				gravity: gravity,
				html: true,
				title: function() { return title(d); },
				opacity: 1
			});
		});
	}

	var title = function(d) { return ""; };

	tooltip.hide = function() { $(".tipsy").remove(); }
	tooltip.title = function(_) { if (arguments.length==0) return title; title = _; return tooltip; }


	return tooltip;
}

var d3LineageContextMenu = function(graph, graphSVG) {

    var onMenuOpen = function(d) {
        handlers.open.call(this, d);
    }

    var onMenuClose = function(d) {
        handlers.close.call(this, d);
    }

    var onMenuClick = function(d) {
        if (d.operation=="viewcode") {
          var url;
          url = g_currentData.nodes[this.__data__]['git_location'];
          if (url){
            window.open(url);
          }
        }
    }

    var onOptionMouseOver = function(d) {

        var items = [];
        if (d.operation=="hideselected") {
            items = graphSVG.selectAll(".node.selected").data();
        }
        if (d.operation=="hideunselected") {
            var items = graphSVG.selectAll(".node").filter(function(d) {
                return !d3.select(this).classed("selected");
            }).data();
        }
        if (d.operation=="hidethis") {
            items = d3.select(this).data();
        }
        if (d.operation=="hidefield" || d.operation=="selectfield") {
            var fieldname = d.fieldname;
            var value = d.value;
            items = graph.getVisibleNodes().filter(function(node) {
                return node.report && node.report[fieldname] && node.report[fieldname][0]==value;
            });
        }
        if (d.operation=="invertselection") {
            var items = graphSVG.selectAll(".node").filter(function(d) {
                return !d3.select(this).classed("selected");
            }).data();
        }
        if (d.operation=="selectall") {
            items = graph.getVisibleNodes();
        }
        if (d.operation=="selectneighbours") {
            var item = d3.select(this).data()[0];
            items = item.getVisibleParents().concat(item.getVisibleChildren());
            items.push(item);
        }
        if (d.operation=="selectpath") {
            items = getEntirePathNodes(d3.select(this).data()[0]);
        }
        handlers.hovernodes.call(this, items);
    }

    var onOptionMouseOut = function(d) {
        onMenuClose(d);
        handlers.hovernodes.call(this, []);
    }

    var ctxmenu = ContextMenu().on("open", onMenuOpen)
                               .on("close", onMenuClose)
                               .on("click", onMenuClick);
                               //.on("mouseover", onOptionMouseOver)
                               //.on("mouseout", onOptionMouseOut);

    var menu = function(selection) {
        menu.hide.call(this, selection);
        selection.each(function(d) {

            var items = [];
            var data;
            data = g_currentData.nodes[d];
            if (data['git_location']){
              items.push({
                "operation": "viewcode",
                "name": "View source code on git"
              });
            }

            items.push({
                "operation": "selectall",
                "name": "Select all"
            });

            items.push({
               "operation": "selectneighbours",
               "name": "Select neighbours"
            });

            items.push({
               "operation": "selectpath",
               "name": "Select path"
            });

            ctxmenu.call(this, items);

            d3.select(this).classed("hascontextmenu", true);
        });
    }

    menu.hide = function(selection) {
        d3.select(this).selectAll(".hascontextmenu").each(function(d) {
            $(this).unbind("contextmenu");
        })
        d3.select(this).selectAll(".context-menu").remove();
    }

    var onhide = function(nodes, selectionname) {}

    var handlers = {
        "hidenodes": function() {},
        "selectnodes": function() {},
        "hovernodes": function() {},
        "open": function() {},
        "close": function() {}
    }

    menu.on = function(event, _) {
        if (!handlers.hasOwnProperty(event)) return menu;
        if (arguments.length==1) return handlers[event];
        handlers[event] = _;
        return menu;
    }

    return menu;
}

var CompareGraphContextMenu = function() {

    var onMenuOpen = function(d) {
        handlers.open.call(this, d);
    }

    var onMenuClick = function(d) {
        if (d.operation=="viewthis") {
            handlers.view.call(this, d3.select(this).datum());
        }
        if (d.operation=="removeselected") {
            handlers.hide.call(this, d3.selectAll(".node.selected").data());
        }
        if (d.operation=="clusterselected") {
            handlers.hide.call(this, d3.selectAll(".node").filter(function(d) {
                return !d3.select(this).classed("selected");
            }).data());
        }
        if (d.operation=="compareselected") {
            handlers.compare.call(this, d3.selectAll(".node.selected").data());
        }
        if (d.operation=="comparetoselected") {
            var ds = d3.selectAll(".node.selected").data().concat(d3.select(this).data());
            handlers.compare.call(this, ds);
        }
    }

    var ctxmenu = ContextMenu().on("click", onMenuClick)
                               .on("open", onMenuOpen);

    var menu = function(selection) {
        menu.hide.call(this, selection);
        selection.each(function(d) {
            var items = [];

            items.push({
                "operation": "viewthis",
                "name": "View execution graph"
            })

            var selected = selection.filter(".selected");
            if (!selected.empty() && (selection[0].length - selected[0].length) > 1) {
                items.push({
                    "operation": "removeselected",
                    "name": "Remove selected from the clustering"
                });
            }

            if (selected[0].length > 1) {
                items.push({
                    "operation": "clusterselected",
                    "name": "Re-cluster using selected"
                });
            }

            if (selected[0].length == 2) {
                items.push({
                    "operation": "compareselected",
                    "name": "Compare graphs of selected"
                });
            }

            if (selected[0].length == 1) {
                if (selected.datum() != d3.select(this).datum()) {
                    items.push({
                        "operation": "comparetoselected",
                        "name": "Compare graph of this to selected"
                    });
                }
            }

            ctxmenu.call(this, items);

            d3.select(this).classed("hascontextmenu", true);
        });
    }

    menu.hide = function(selection) {
        d3.select(this).selectAll(".hascontextmenu").each(function(d) {
            $(this).unbind("contextmenu");
        })
        d3.select(this).selectAll(".context-menu").remove();
    }

    var handlers = {
        "open": function() {},
        "view": function() {},
        "hide": function() {},
        "compare": function() {}
    }

    menu.on = function(event, _) {
        if (!handlers.hasOwnProperty(event)) return menu;
        if (arguments.length==1) return handlers[event];
        handlers[event] = _;
        return menu;
    }

    return menu;
}

var ContextMenu = function() {

    var idseed = 0;

    var menu = function(ds) {
        var attach = this;

        // Create the menu items
        var menu = {};
        for (var i = 0; i < ds.length; i++) {
            var item = ds[i];
            var itemname = name.call(this, item);
            menu[itemname] = {
                "click": menuClick(attach, item, i),
                "mouseover": menuMouseOver(attach, item, i),
                "mouseout": menuMouseOut(attach, item, i)
            };
        }

        // Set the options
        var options = {
            "disable_native_context_menu": true,
            "showMenu": function() { handlers.open.call(attach, ds); },
            "hideMenu": function() { handlers.close.call(attach, ds); }
        }

        // Attach the context menu to this element
        $(attach).contextMenu('context-menu'+(idseed++), menu, options);
    }

    // Stupid javascript
    var menuClick = function(attach,d, i) {
        return function() {
            handlers.click.call(attach, d, i);
        }
    }

    // Stupid stupid javascript
    var menuMouseOver = function(attach, d, i) {
        return function() {
            handlers.mouseover.call(attach, d, i);
        }
    }

    // Stupid stupid stupid javascript
    var menuMouseOut = function(attach, d, i) {
        return function() {
            handlers.mouseout.call(attach, d, i);
        }
    }

    var name = function(d) { return d.name; }

    var handlers = {
        "click": function() {},
        "open": function() {},
        "close": function() {},
        "mouseover": function() {},
        "mouseout": function() {},
    }


    menu.name = function(_) { if (arguments.length==0) return name; name = _; return menu; }
    menu.on = function(event, _) {
        if (!handlers[event]) return menu;
        if (arguments.length==1) return handlers[event];
        handlers[event] = _;
        return menu;
    }


    return menu;
}