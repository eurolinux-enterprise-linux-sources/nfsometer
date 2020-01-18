"""
Copyright 2012 NetApp, Inc. All Rights Reserved,
contribution by Weston Andros Adamson <dros@netapp.com>

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; either version 2 of the License, or (at your option) any later
version.
This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
"""

from config import *

SELECTOR_ORDER=(
    'workload',
    'kernel',
    'mountopt',
    'detect',
    'tag',
    'client',
    'server',
    'path',
)

_valid_things = set(SELECTOR_ORDER)

def _fmt(name, x, default=None, short=True, sep=' '):
    if isinstance(x, (list, tuple)):
        return sep.join([ _fmt(name, y) for y in x ])
    return x

class Selector(object):
    """ This class is used to specify selection of the current query
    """
    def __init__(self, *args):
        things = []

        assert len(args) == len(SELECTOR_ORDER)

        for idx, name in enumerate(SELECTOR_ORDER):
            obj = args[idx]
            if not isinstance(obj, (list, tuple)):
                obj = [obj]

            setattr(self, name + 's', tuple(obj))

    def __str__(self):
        out = []

        for name in SELECTOR_ORDER:
            obj = getattr(self, name + 's')
            out.append('%s%s: %s' % (name, pluralize(len(obj)), ', '.join(obj)))

        return ', '.join(out)

    def __hash__(self):
        args = []

        for name in SELECTOR_ORDER:
            obj = getattr(self, name + 's')
            assert len(obj) == 1, \
                "Can't hash selector with %s length != 1 - %r" % (name, obj)
            args.append(obj)

        return hash(tuple(args))

    def __cmp__(self, other):
        for name in SELECTOR_ORDER:
            r = cmp(getattr(self, name + 's'), getattr(other, name + 's'))
            if r:
                return r
        return 0

    def compare_order(self, other, order):
        for name in order:
            r = cmp(getattr(self, name + 's'), getattr(other, name + 's'))
            if r:
                return r
        return 0

    def __repr__(self):
        args = []

        for name in SELECTOR_ORDER:
            obj = getattr(self, name + 's')
            args.append(obj)

        return 'Selector(%s)' % (', '.join([repr(x) for x in args]),)

    def __getattr__(self, attr):
        superself = super(Selector, self)
        if attr in SELECTOR_ORDER:
            obj = getattr(self, attr + 's')
            assert len(obj) == 1, "%s is not singular" % attr
            return obj[0]
        elif hasattr(superself, attr):
            return getattr(superself, attr)
        else:
            raise AttributeError, "invalid attribute: %r" % attr

    def __add__(self, other):
        for name in SELECTOR_ORDER:
            vals = []
            vals.extend(obj)
            vals.extend(getattr(other, name + 's'))
            vals = set(vals)

            setattr(self, name + 's', vals)

    def html(self):
        out = []

        for name in SELECTOR_ORDER:
            obj = getattr(self, name + 's')
            out.append('%s%s: %s' % (name, pluralize(len(obj)), ', '.join(obj)))

        return '<br>'.join(out)

    def is_valid_key(self):
        for name in SELECTOR_ORDER:
            obj = getattr(self, name + 's')
            if len(obj) != 1:
                return False
        return True

    def _foreach_thing(self, thing):
        assert thing

        if isinstance(thing, (list, tuple)):
            thing = list(thing)
            more_things = thing[1:]
            thing = thing[0]
        else:
            assert thing in _valid_things
            more_things = []

        for x in getattr(self, thing + 's'):
            args = []

            for name in SELECTOR_ORDER:
                if name == thing:
                    obj = x
                else:
                    obj = getattr(self, name + 's')
                args.append(obj)

            sel = Selector(*args)

            if more_things:
                for y in sel._foreach_thing(more_things):
                    yield y
            else:
                yield sel

    def foreach(self, thing=None):
        if thing == None:
            thing = SELECTOR_ORDER

        for x in self._foreach_thing(thing):
            yield x

    def fmt(self, thing, short=True, title=False):
        assert thing in _valid_things
        x = getattr(self, thing + 's')
        return _fmt(thing, x, default = lambda x : ' '.join(x), short=short)

    def title(self, thing):
        assert thing in _valid_things
        x = getattr(self, thing + 's')
        return "%s%s" % (thing, pluralize(len(obj)))

    def display_info(self, all_selector, show_all=False, sep=' ',
                     pre='', post=''):
        display_info = []

        for name in SELECTOR_ORDER:
            obj = getattr(self, name + 's')
            all_obj = getattr(all_selector, name + 's')

            if show_all or obj != all_obj:
                pl = pluralize(len(obj))
                entry = ('%s%s' % (name, pl),
                         pre + str(_fmt(name, obj, sep=sep)) + post)
                display_info.append(entry)

        return display_info

