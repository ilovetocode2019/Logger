"""
Copyright (c) 2020 Fyssion

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at https://mozilla.org/MPL/2.0/.
"""


import asyncio
import inspect
import functools
from collections import OrderedDict


class LRUDict(OrderedDict):
    """A dict with a maximum length which removes items in LRU fashon.

    This inherits from :class:`collections.OrderedDict`
    All args and kwargs passed into the constructor are
    passed to :class:`collections.OrderedDict`

    Parameters
    -----------
    max_len: Optional[:class:`int`]
        The maximum length of the dict.
        When this length is exceeded, items are
        removed in LRU fashon.
        Defaults to 10
    """

    def __init__(self, max_len: int = 10, *args, **kwargs):
        if max_len <= 0:
            raise ValueError()
        self.max_len = max_len

        super().__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        super().move_to_end(key)

        while len(self) > self.max_len:
            oldkey = next(iter(self))
            super().__delitem__(oldkey)

    def __getitem__(self, key):
        val = super().__getitem__(key)
        super().move_to_end(key)

        return val


def _prepare_coro(coro, cache, key):
    # This wraps a coroutine so we can call it
    # and then store it's value to the cache
    async def func():
        value = await coro
        cache[key] = value
        return value

    return func()


def _wrap_value(value):
    async def func():
        return value

    return func()


class Cache:
    """A cache that wraps a func and uses :class:`LRUDict` to store the returning value.

    Don't create an instance of this directly.
    Instead, use the provided :func:`cache` decorator.

    You can use the wrapped function like you normally would. The difference is before calling
    the function, a lookup is performed on the internal LRUDict. If a value is found, that value
    is returned instead of thefunction being called again. This is useful for avoiding unnecessary
    processing. If the value is not found in the internal LRUDict, the function is called and the
    return value is stored in the dict.

    Each key is formatted as the reprs of all the args (and optionally kwargs) joined by ':'.
    The module and function names are prepended.
    For example, say the args are ("ex", 2), the module is "test" and the function is "timestwo".
    The key would be "test.timestwo:ex:2".

    To invalidate one or more keys, use :meth:`invalidate` or :meth:`invalidate_containing`.

    Usage example: ::

        @cache()
        async def fetch_config(user_id):
            # do any work here as normal
            # example:
            record =  await pool.fetchrow("SELECT * FROM user_config WHERE user_id=$1", user_id)

            if not record:
                return None

            return UserConfig.from_record(record)

        # Elsewhere...
        config = await fetch_config(1234)

        # Invalidating the item with 1234 as the arg
        fetch_config.invalidate(1234)

        # Invalidating all items containing "1"
        fetch_config.invalidate_containing("1")

        # Invalidating the entire cache
        fetch_config.invalidate()

    Parameters
    -----------
    func:
        The function to wrap.
    max_len: :class:`int`
        The maximum length of the cache.
        Defaults to 128.
    ignore_kwargs: :class:`bool`
        Whether to ignore kwargs when resolving the key
    """

    def __init__(self, func, max_len: int = 128, ignore_kwargs=False):
        self.func = func
        self.ignore_kwargs = ignore_kwargs
        self.cache = LRUDict(max_len)

    def __len__(self):
        return len(self.cache)

    def _resolve_key(self, args, kwargs):
        key = [f"{self.func.__module__}.{self.func.__name__}"]
        key.extend(repr(a) for a in args)

        if not self.ignore_kwargs:
            for k, v in kwargs.items():
                key.append(repr(k))
                key.append(repr(v))

        return ":".join(key)

    def invalidate(self, *args, **kwargs):
        """Remove an item from the cache with the provided args and kwargs or the entire cache if none provided.

        Returns
        --------
        :class:`bool`
            Whether or not the invalidate succeeded.
        """
        if not args and not kwargs:
            self.cache.clear()
            return True

        key = self._resolve_key(args, kwargs)

        try:
            del self.cache[key]
            return True

        except KeyError:
            return False

    def invalidate_containing(self, key):
        """Invalidate all items from the cache containing the key provided.

        Parameters
        -----------
        key: :class:`str`
            A string to use to invalidate items in the dict.
        """
        to_remove = []

        for k in self.cache.keys():
            if key in k:
                to_remove.append(k)

        for key in to_remove:
            try:
                del self.cache[key]
            except KeyError:
                continue

    def __call__(self, *args, **kwargs):
        key = self._resolve_key(args, kwargs)

        try:
            value = self.cache[key]

        except KeyError:
            # Add the value to the cache since it isn't there
            value = self.func(*args, **kwargs)

            if inspect.isawaitable(value):
                return _prepare_coro(value, self.cache, key)

            self.cache[key] = value
            return value

        else:
            if asyncio.iscoroutinefunction(self.func):
                return _wrap_value(value)
            return value


def cache(*args, **kwargs):
    """Decorator that turns a function into a :class:`Cache`."""

    def decorator(func):
        cache = Cache(func, *args, **kwargs)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return cache(*args, **kwargs)

        return wrapper

    return decorator
