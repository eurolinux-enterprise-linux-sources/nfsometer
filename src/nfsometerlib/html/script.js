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

function info_mode(obj) {
    var ds = $(obj).parents('div[class=dataset]');
    var show_button = ds.find('input[class=dataset_info_show_button]');
    var hide_button = ds.find('input[class=dataset_info_hide_button]');
    var infos = ds.find('tr[class=data_info_pane]');
    var label_td = ds.find('td[class*=label]');

    infos.show();
    show_button.hide();
    hide_button.show();
    label_td.addClass('selected');

    return;
}

function table_mode(obj) {
    var ds = $(obj).parents('div[class=dataset]');
    var show_button = ds.find('input[class=dataset_info_show_button]');
    var hide_button = ds.find('input[class=dataset_info_hide_button]');
    var infos = ds.find('tr[class=data_info_pane]');
    var label_td = ds.find('td[class*=label]');

    infos.hide();
    show_button.show();
    hide_button.hide();
    label_td.removeClass('selected');

    return;
}

function show_data_info(obj) {
    var label_td = $(obj)
    var pane_id = label_td.find('input[name*="pane_id"]').attr('value');
    var ds = label_td.parents('div[class=dataset]');
    var show_button = ds.find('input[class=dataset_info_show_button]');
    var hide_button = ds.find('input[class=dataset_info_hide_button]');
    var infos = ds.find('tr[class=data_info_pane]');
    var sel = ds.find('tr[id*="' + pane_id + '"]');

    if (sel.is(":visible")) {
        sel.hide();
        label_td.removeClass('selected');
    } else {
        sel.show();
        label_td.addClass('selected');
    }

    if (infos.find(":visible").length) {
        show_button.hide();
        hide_button.show();
    } else {
        show_button.show();
        hide_button.hide();
    }
}

function show_data_nfsvers(obj) {
    var nfsvers = $(obj).find('option:selected').attr('value');
    var ds = $(obj).parents('div[class=dataset]');

    var newsrc = ds.find('input[name="data_graph_' + nfsvers + '"]').attr('value');
    var img = ds.find('img[class="data_graph"]');

    img.attr('src', newsrc);
}


$(document).ready(function() {
    $('.nfsvers').change(function(){
        show_data_nfsvers(this);
    });
    $('.label').click(function(){
        show_data_info(this);
    });
    $('input.dataset_info_show_button').click(function(){
        info_mode(this);
    });
    $('input.dataset_info_hide_button').click(function(){
        table_mode(this);
    });
});
