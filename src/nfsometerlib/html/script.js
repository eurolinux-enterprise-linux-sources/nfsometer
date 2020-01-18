/*
 * Copyright 2012 NetApp, Inc. All Rights Reserved,
 * contribution by Weston Andros Adamson <dros@netapp.com>
 * 
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 2 of the License, or (at your option) any later
 * version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 */

function table_view(obj) {
    var view = $(obj).find('option:selected').attr('value');
    var ds = $(obj).parents('div[class=dataset]');

    var all_divs = ds.find('div[class^=compare_]');
    var this_divs = ds.find('div[class=compare_' + view + ']');

    var label_detail = ds.find('div[class=group_normal]')
    var label_normal = ds.find('div[class=group_detail]')

    if (view == 'rundata') {
        label_detail.hide();
        label_normal.show();
    } else {
        label_normal.hide();
        label_detail.show();
    }

    all_divs.hide();
    this_divs.show();
}

function graph_view(obj) {
    var nfsvers = $(obj).find('option:selected').attr('value');
    var ds = $(obj).parents('div[class=dataset]');

    var newsrc = ds.find('input[name="data_graph_' + nfsvers + '"]').attr('value');
    var img = ds.find('img[class="data_graph"]');

    img.attr('src', newsrc);
}

$(document).ready(function() {
    $('.graph_view').change(function(){
        graph_view(this);
    });
    $('.table_view').change(function(){
        table_view(this);
    });
});
