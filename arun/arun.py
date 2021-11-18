#!/usr/bin/env python3
import os
import traceback
import signal
import logging
import asyncio
import functools
from types import CoroutineType
from contextlib import asynccontextmanager


_tasks = []
_init_tasks = []
_cleanup_tasks = []
_main_loop = None
_init_fail_exit = True


_logger = None


async def _arun_task(task):
    try:
        return await task
    except asyncio.CancelledError:
        pass
    except Exception as e:
        _logger.warning(f'\n\n')
        _logger.warning(f'task: {task} exception logging begin')
        _logger.warning(f'task: {task} exception: {repr(e)}')
        _logger.warning(f'trace {traceback.format_exc()}')
        _logger.warning(f'task: {task} exception longging end\n\n')


def append_task(*args):
    for task in args:
        assert(type(task) is CoroutineType)
    _tasks.extend([_arun_task(task) for task in args])


async def _arun_init(task):
    try:
        await task
    except asyncio.CancelledError:
        pass
    except Exception as e:
        _logger.warning(f'\n\n')
        _logger.error(f'init: {task} exception logging begin')
        _logger.error(f'init: {task} exception: {repr(e)}')
        _logger.error(f'trace {traceback.format_exc()}')
        _logger.error(f'init: {task} exception logging end\n\n')
        if _init_fail_exit:
            os._exit(123)


def append_init(*args):
    for task in args:
        assert(type(task) is CoroutineType)
    _init_tasks.extend([_arun_init(task) for task in args])


async def _arun_cleanup(task):
    try:
        await task
    except asyncio.CancelledError:
        pass
    except Exception as e:
        _logger.warning(f'\n\n')
        _logger.error(f'cleanup: {task} exception logging begin')
        _logger.error(f'cleanup: {task} exception: {repr(e)}')
        _logger.error(f'trace {traceback.format_exc()}')
        _logger.error(f'cleanup: {task} exception logging end\n\n')


def append_cleanup(*args):
    for task in args:
        assert(type(task) is CoroutineType)
    _cleanup_tasks.extend([_arun_cleanup(task) for task in args])


def future():
    loop = asyncio.get_running_loop()
    assert(loop == _main_loop)
    return loop.create_future()


def post_in_main(task):
    assert(type(task) is CoroutineType)
    loop = None
    try:
        loop = asyncio.get_running_loop()
    except Exception:
        pass
    assert(loop != _main_loop)
    return asyncio.run_coroutine_threadsafe(task, _main_loop)


def exec_in_main(task):
    return post_in_main(task).result()


def post_in_thread(thread_proc, *args, **kwargs):
    loop = asyncio.get_running_loop()
    assert(loop == _main_loop)
    p_proc = functools.partial(thread_proc, *args, **kwargs)
    return loop.run_in_executor(None, p_proc)


def post_in_task(task):
    assert(type(task) is CoroutineType)
    loop = asyncio.get_running_loop()
    assert(loop == _main_loop)
    return asyncio.create_task(_arun_task(task))


def post_in_shell(cmd, err=None):
    loop = asyncio.get_running_loop()
    assert(loop == _main_loop)
    if err is not None:
        err = asyncio.subprocess.PIPE
    return asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=err)


def sleep(t):
    loop = asyncio.get_running_loop()
    assert(loop == _main_loop)
    return asyncio.sleep(t)


@asynccontextmanager
async def timeout(t):
    loop = asyncio.get_running_loop()
    assert(loop == _main_loop)
    c_task = asyncio.current_task()
    c_lock = asyncio.Lock()

    async def _time_guard(t):
        nonlocal c_task
        nonlocal c_lock
        await sleep(t)
        async with c_lock:
            if c_task is not None:
                tmp, c_task = c_task, None
                tmp.cancel()

    t_guard = post_in_task(_time_guard(t))

    try:
        yield
    except asyncio.CancelledError:
        async with c_lock:
            if c_task is None:
                raise asyncio.TimeoutError from None
            else:
                raise
    finally:
        async with c_lock:
            if c_task is not None:
                c_task = None
                t_guard.cancel()


async def _wait_forever():
    await future()


async def _cleanup_all():
    for task in reversed(_cleanup_tasks):
        await task


async def _init_all():
    for task in _init_tasks:
        await task


_cancelled_tasks = []


async def _shutdown(signal):
    _logger.info(f'Received exit signal {signal.name}')
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    _logger.info(f'Cancelling {len(tasks)} outstanding tasks')
    for task in tasks:
        task.cancel()
        _cancelled_tasks.append(task)


async def _wait_for_cancelled():
    for task in _cancelled_tasks:
        try:
            await task
        except asyncio.CancelledError:
            pass


async def try_until_done(ttl, gap, tfunc, *args, **kwargs):
    async with timeout(ttl):
        while True:
            try:
                ret = await tfunc(*args, **kwargs)
                break
            except asyncio.CancelledError:
                raise
            except Exception:
                await sleep(gap)
    return ret


def post_exit():
    return post_in_main(_shutdown(signal.SIGTERM))


def loop():
    global _main_loop
    assert(_main_loop is not None)
    return _main_loop


def run(loglevel=logging.DEBUG, forever=False, init_fail_exit=True):
    global _main_loop
    global _logger
    global _init_fail_exit

    assert(_main_loop is None and _logger is None)

    _init_fail_exit = init_fail_exit
    _logger = logging.getLogger(__name__)
    _main_loop = asyncio.get_event_loop()
    logging.basicConfig(level=loglevel, format='%(asctime)s %(message)s')
    loop = _main_loop

    # run init
    _logger.info(f'Running init tasks')
    loop.run_until_complete(_init_all())
    try:  # run task with signal-handle
        _logger.info(f'Install signal-handlers')
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT, signal.SIGUSR1)
        try:
            for s in signals:
                loop.add_signal_handler(s, lambda s=s: post_in_task(_shutdown(s)))
        except Exception:
            pass
        _logger.info(f'Runing normal tasks')
        if forever:
            append_task(_wait_forever())
        loop.run_until_complete(asyncio.gather(*_tasks))
        _logger.info(f'Remove signal-handlers')
        for s in signals:
            loop.remove_signal_handler(s)
        _logger.info(f'Wait for cancelled tasks')
        loop.run_until_complete(_wait_for_cancelled())
    finally:  # cleanup
        _logger.info(f'Running cleanup tasks')
        loop.run_until_complete(_cleanup_all())
        loop.close()
        _logger.info('Successfully shutdown')


if __name__ == '__main__':
    async def _test(t):
        await sleep(t)
        print(f'time out {t}')

    async def _clean_test(i):
        print(f'test-clean{i}')
        raise NameError('test cleanup exception')

    async def _init_test(i):
        print(f'init-clean{i}')
        await sleep(i)
        raise NameError('test init exception')

    async def _test_post_task():
        print('test-post-task')
        try:
            async with timeout(1):
                await sleep(5)
                print('first timeout 1. not print')
        except Exception as e:
            print(f'first timeout {repr(e)}')

        try:
            async with timeout(1000):
                await sleep(15)
                print('second timeout. should print')
        except Exception as e:
            print(f'sencond timeout {repr(e)}')

    async def _test_except(t):
        await sleep(t)
        post_in_task(_test_post_task())
        print(f'time out {t}')
        raise NameError('test_except')

    append_task(_test(3), _test(1), _test(2), _test_except(5))
    append_cleanup(_clean_test(1), _clean_test(2), _clean_test(3))
    append_init(_init_test(1), _init_test(2), _init_test(3))
    run(forever=True, init_fail_exit=False)
