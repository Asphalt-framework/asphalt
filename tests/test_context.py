from __future__ import annotations

import sys
from collections.abc import AsyncGenerator, Callable
from itertools import count
from typing import Any, NoReturn, Optional, Tuple, Union

import pytest
from anyio import create_task_group, wait_all_tasks_blocked

from asphalt.core import (
    Context,
    NoCurrentContext,
    ResourceConflict,
    ResourceNotFound,
    context_teardown,
    current_context,
    get_resource,
    get_resource_nowait,
    inject,
    resource,
)

if sys.version_info < (3, 11):
    from exceptiongroup import ExceptionGroup

pytestmark = pytest.mark.anyio()


@pytest.fixture
def context() -> Context:
    return Context()


class TestContext:
    async def test_parent(self) -> None:
        """
        Test that the parent property points to the parent context instance, if any.

        """
        async with Context() as parent:
            async with Context() as child:
                assert parent.parent is None
                assert child.parent is parent

    @pytest.mark.parametrize(
        "exception", [None, Exception("foo")], ids=["noexception", "exception"]
    )
    async def test_close(self, context: Context, exception: Exception | None) -> None:
        """
        Test that teardown callbacks are called in reverse order when a context is
        closed.

        """

        def callback(exception=None):
            called_functions.append((callback, exception))

        async def async_callback(exception=None):
            called_functions.append((async_callback, exception))

        called_functions: list[tuple[Callable, BaseException | None]] = []
        context.add_teardown_callback(callback, pass_exception=True)
        context.add_teardown_callback(async_callback, pass_exception=True)
        await context.close(exception)

        assert called_functions == [(async_callback, exception), (callback, exception)]

    async def test_close_while_running_teardown(self, context: Context) -> None:
        """
        Test that trying to close the context from a teardown callback raises a
        RuntimeError.
        """

        async def try_close_context() -> None:
            with pytest.raises(RuntimeError, match="this context is already closing"):
                await context.close()

        context.add_teardown_callback(try_close_context)
        await context.close()

    async def test_teardown_callback_exception(self, context: Context) -> None:
        """
        Test that all callbacks are called even when some teardown callbacks raise
        exceptions, and that those exceptions are reraised in such a case.

        """

        def callback1() -> None:
            items.append(1)

        def callback2() -> NoReturn:
            raise Exception("foo")

        context.add_teardown_callback(callback1)
        context.add_teardown_callback(callback2)
        context.add_teardown_callback(callback1)
        context.add_teardown_callback(callback2)
        items: list[int] = []
        with pytest.raises(ExceptionGroup) as exc:
            await context.close()

        assert len(exc.value.exceptions) == 2

    async def test_close_closed(self, context: Context) -> None:
        """Test that closing an already closed context raises a RuntimeError."""
        assert not context.closed
        await context.close()
        assert context.closed

        with pytest.raises(RuntimeError) as exc:
            await context.close()

        exc.match("this context has already been closed")

    @pytest.mark.parametrize("types", [int, (int,), ()], ids=["type", "tuple", "empty"])
    async def test_add_resource(self, context, types):
        """
        Test that a resource is properly added in the context and listeners are
        notified.

        """
        async with create_task_group() as tg:
            tg.start_soon(context.add_resource, 6, "foo", types)
            event = await context.resource_added.wait_event()

        assert event.resource_types == (int,)
        assert event.resource_name == "foo"
        assert not event.is_factory
        assert context.get_resource_nowait(int, "foo") == 6

    async def test_add_resource_name_conflict(self, context: Context) -> None:
        """Test that adding a resource won't replace any existing resources."""
        await context.add_resource(5, "foo")
        with pytest.raises(ResourceConflict) as exc:
            await context.add_resource(4, "foo")

        exc.match(
            "this context already contains a resource of type int using the name 'foo'"
        )

    async def test_add_resource_none_value(self, context: Context) -> None:
        """Test that None is not accepted as a resource value."""
        with pytest.raises(ValueError, match='"value" must not be None'):
            await context.add_resource(None)

    async def test_add_resource_type_conflict(self, context: Context) -> None:
        await context.add_resource(5)
        with pytest.raises(ResourceConflict) as exc:
            await context.add_resource(6)

        exc.match(
            "this context already contains a resource of type int using the name "
            "'default'"
        )

    @pytest.mark.parametrize(
        "name", ["a.b", "a:b", "a b"], ids=["dot", "colon", "space"]
    )
    async def test_add_resource_bad_name(self, context, name):
        with pytest.raises(ValueError) as exc:
            await context.add_resource(1, name)

        exc.match(
            '"name" must be a nonempty string consisting only of alphanumeric '
            "characters and underscores"
        )

    async def test_add_resource_factory(self, context: Context) -> None:
        """
        Test that resource factory callbacks are only called once for each context.

        """

        def factory(ctx):
            assert ctx is context
            return next(counter)

        counter = count(1)
        await context.add_resource_factory(factory, types=[int])

        assert context.get_resource_nowait(int) == 1
        assert context.get_resource_nowait(int) == 1

    @pytest.mark.parametrize(
        "name", ["a.b", "a:b", "a b"], ids=["dot", "colon", "space"]
    )
    async def test_add_resource_factory_bad_name(self, context, name):
        with pytest.raises(ValueError) as exc:
            await context.add_resource_factory(lambda ctx: 1, name, types=[int])

        exc.match(
            '"name" must be a nonempty string consisting only of alphanumeric '
            "characters and underscores"
        )

    async def test_add_resource_factory_empty_types(self, context: Context) -> None:
        with pytest.raises(ValueError) as exc:
            await context.add_resource_factory(lambda ctx: 1, types=())

        exc.match("no resource types were specified")

    async def test_add_resource_factory_type_conflict(self, context: Context) -> None:
        await context.add_resource_factory(lambda ctx: None, types=(str, int))
        with pytest.raises(ResourceConflict) as exc:
            await context.add_resource_factory(lambda ctx: None, types=[int])

        exc.match("this context already contains a resource factory for the type int")

    async def test_add_resource_factory_no_inherit(self, context: Context) -> None:
        """
        Test that a subcontext gets its own version of a factory-generated resource even
        if a parent context has one already.

        """
        await context.add_resource_factory(id, types=[int])

        async with context, Context() as subcontext:
            assert context.get_resource_nowait(int) == id(context)
            assert subcontext.get_resource_nowait(int) == id(subcontext)

    async def test_add_resource_return_type_single(self, context: Context) -> None:
        def factory(ctx: Context) -> str:
            return "foo"

        async with context:
            await context.add_resource_factory(factory)
            assert context.get_resource_nowait(str) == "foo"

    async def test_add_resource_return_type_union(self, context: Context) -> None:
        def factory(ctx: Context) -> Union[int, float]:  # noqa: UP007
            return 5

        async with context:
            await context.add_resource_factory(factory)
            assert context.get_resource_nowait(int) == 5
            assert context.get_resource_nowait(float) == 5

    @pytest.mark.skipif(sys.version_info < (3, 10), reason="Requires Python 3.10+")
    async def test_add_resource_return_type_uniontype(self, context: Context) -> None:
        def factory(ctx: Context) -> int | float:
            return 5

        async with context:
            await context.add_resource_factory(factory)
            assert context.get_resource_nowait(int) == 5
            assert context.get_resource_nowait(float) == 5

    async def test_add_resource_return_type_optional(self, context: Context) -> None:
        def factory(ctx: Context) -> Optional[str]:  # noqa: UP007
            return "foo"

        async with context:
            await context.add_resource_factory(factory)
            assert context.get_resource_nowait(str) == "foo"

    async def test_get_static_resources(self, context: Context) -> None:
        await context.add_resource(9, "foo")
        await context.add_resource_factory(lambda ctx: 7, "bar", types=[int])
        async with context, Context() as subctx:
            await subctx.add_resource(1, "bar")
            await subctx.add_resource(4, "foo")
            assert subctx.get_static_resources(int) == {1, 4, 9}

    async def test_require_resource(self, context: Context) -> None:
        await context.add_resource(1)
        assert context.get_resource_nowait(int) == 1

    def test_require_resource_not_found(self, context: Context) -> None:
        """
        Test that ResourceNotFound is raised when a required resource is not found.

        """
        exc = pytest.raises(ResourceNotFound, context.get_resource_nowait, int, "foo")
        exc.match("no matching resource was found for type=int name='foo'")
        assert exc.value.type == int
        assert exc.value.name == "foo"


class TestContextTeardown:
    @pytest.mark.parametrize(
        "expected_exc", [None, Exception("foo")], ids=["no_exception", "exception"]
    )
    async def test_function(self, expected_exc: Exception | None) -> None:
        phase = received_exception = None

        @context_teardown
        async def start(ctx: Context) -> AsyncGenerator[None, Any]:
            nonlocal phase, received_exception
            phase = "started"
            exc = yield
            phase = "finished"
            received_exception = exc

        context = Context()
        await start(context)
        assert phase == "started"

        await context.close(expected_exc)
        assert phase == "finished"
        assert received_exception == expected_exc

    @pytest.mark.parametrize(
        "expected_exc", [None, Exception("foo")], ids=["no_exception", "exception"]
    )
    async def test_method(self, expected_exc: Exception | None) -> None:
        phase = received_exception = None

        class SomeComponent:
            @context_teardown
            async def start(self, ctx: Context) -> AsyncGenerator[None, Any]:
                nonlocal phase, received_exception
                phase = "started"
                exc = yield
                phase = "finished"
                received_exception = exc

        context = Context()
        await SomeComponent().start(context)
        assert phase == "started"

        await context.close(expected_exc)
        assert phase == "finished"
        assert received_exception == expected_exc

    def test_plain_function(self) -> None:
        def start(ctx) -> None:
            pass

        pytest.raises(TypeError, context_teardown, start).match(
            " must be an async generator function"
        )

    async def test_exception(self) -> None:
        @context_teardown
        async def start(ctx: Context) -> AsyncGenerator[None, Any]:
            raise Exception("dummy error")
            yield

        context = Context()
        with pytest.raises(Exception) as exc_info:
            await start(context)

        exc_info.match("dummy error")

    async def test_get_resource_at_teardown(self) -> None:
        resource = ""

        async def teardown_callback() -> None:
            nonlocal resource
            resource = await get_resource(str)

        async with Context() as ctx:
            await ctx.add_resource("blah")
            ctx.add_teardown_callback(teardown_callback)

        assert resource == "blah"

    async def test_generate_resource_at_teardown(self) -> None:
        resource = ""

        async def teardown_callback() -> None:
            nonlocal resource
            resource = await get_resource(str)

        async with Context() as ctx:
            await ctx.add_resource_factory(lambda context: "blah", types=[str])
            ctx.add_teardown_callback(teardown_callback)

        assert resource == "blah"


class TestContextFinisher:
    @pytest.mark.parametrize(
        "expected_exc", [None, Exception("foo")], ids=["no_exception", "exception"]
    )
    async def test_context_teardown(self, expected_exc: Exception | None) -> None:
        phase = received_exception = None

        @context_teardown
        async def start(ctx: Context) -> AsyncGenerator[None, Any]:
            nonlocal phase, received_exception
            phase = "started"
            exc = yield
            phase = "finished"
            received_exception = exc

        context = Context()
        await start(context)
        assert phase == "started"

        await context.close(expected_exc)
        assert phase == "finished"
        assert received_exception == expected_exc


async def test_current_context() -> None:
    pytest.raises(NoCurrentContext, current_context)

    async with Context() as parent_ctx:
        assert current_context() is parent_ctx
        async with Context() as child_ctx:
            assert current_context() is child_ctx

        assert current_context() is parent_ctx

    pytest.raises(NoCurrentContext, current_context)


async def test_get_resource() -> None:
    async with Context() as ctx:
        await ctx.add_resource("foo")
        assert await get_resource(str) == "foo"
        assert await get_resource(int, optional=True) is None


async def test_get_resource_sync() -> None:
    async with Context() as ctx:
        await ctx.add_resource("foo")
        assert get_resource_nowait(str) == "foo"
        assert get_resource_nowait(int, optional=True) is None


async def test_context_stack_corruption(anyio_backend_name: str) -> None:
    async def generator() -> AsyncGenerator[None, None]:
        async with Context():
            yield

    if anyio_backend_name == "asyncio":
        pytest.xfail("Won't work before AnyIO 4.2.1")

    gen = generator()
    async with create_task_group() as tg:
        tg.start_soon(gen.asend, None)  # type: ignore[arg-type]
        await wait_all_tasks_blocked()
        with pytest.warns(
            UserWarning, match="Potential context stack corruption detected"
        ):
            try:
                await gen.asend(None)
            except StopAsyncIteration:
                pass

    pytest.raises(NoCurrentContext, current_context)


class TestDependencyInjection:
    async def test_static_resources(self) -> None:
        @inject
        async def injected(
            foo: int, bar: str = resource(), *, baz: str = resource("alt")
        ) -> Tuple[int, str, str]:  # noqa: UP006
            return foo, bar, baz

        async with Context() as ctx:
            await ctx.add_resource("bar_test")
            await ctx.add_resource("baz_test", "alt")
            foo, bar, baz = await injected(2)

        assert foo == 2
        assert bar == "bar_test"
        assert baz == "baz_test"

    async def test_sync_injection(self) -> None:
        @inject
        def injected(
            foo: int, bar: str = resource(), *, baz: str = resource("alt")
        ) -> Tuple[int, str, str]:  # noqa: UP006
            return foo, bar, baz

        async with Context() as ctx:
            await ctx.add_resource("bar_test")
            await ctx.add_resource("baz_test", "alt")
            foo, bar, baz = injected(2)

        assert foo == 2
        assert bar == "bar_test"
        assert baz == "baz_test"

    async def test_missing_annotation(self) -> None:
        async def injected(
            foo: int, bar: str = resource(), *, baz=resource("alt")
        ) -> None:
            pass

        pytest.raises(TypeError, inject, injected).match(
            f"Dependency for parameter 'baz' of function "
            f"'{__name__}.{self.__class__.__name__}.test_missing_annotation.<locals>"
            f".injected' is missing the type annotation"
        )

    async def test_missing_resource(self) -> None:
        @inject
        async def injected(foo: int, bar: str = resource()) -> None:
            pass

        with pytest.raises(ResourceNotFound) as exc:
            async with Context():
                await injected(2)

        exc.match("no matching resource was found for type=str name='default'")

    @pytest.mark.parametrize(
        "annotation",
        [
            pytest.param(Optional[str], id="optional"),
            # pytest.param(Union[str, int, None], id="union"),
            pytest.param(
                "str | None",
                id="uniontype.10",
                marks=[
                    pytest.mark.skipif(
                        sys.version_info < (3, 10), reason="Requires Python 3.10+"
                    )
                ],
            ),
        ],
    )
    @pytest.mark.parametrize(
        "sync",
        [
            pytest.param(True, id="sync"),
            pytest.param(False, id="async"),
        ],
    )
    async def test_inject_optional_resource_async(
        self, annotation: type, sync: bool
    ) -> None:
        if sync:

            @inject
            def injected(
                res: annotation = resource(),  # type: ignore[valid-type]
            ) -> annotation:  # type: ignore[valid-type]
                return res

        else:

            @inject
            async def injected(
                res: annotation = resource(),  # type: ignore[valid-type]
            ) -> annotation:  # type: ignore[valid-type]
                return res

        async with Context() as ctx:
            retval: Any = injected() if sync else (await injected())
            assert retval is None
            await ctx.add_resource("hello")
            retval = injected() if sync else (await injected())
            assert retval == "hello"

    def test_resource_function_not_called(self) -> None:
        async def injected(
            foo: int,
            bar: str = resource,  # type: ignore[assignment]
        ) -> None:
            pass

        with pytest.raises(TypeError) as exc:
            inject(injected)

        exc.match(
            f"Default value for parameter 'bar' of function "
            f"{__name__}.{self.__class__.__name__}.test_resource_function_not_called"
            f".<locals>.injected was the 'resource' function – did you forget to add "
            f"the parentheses at the end\\?"
        )

    def test_missing_inject(self) -> None:
        def injected(foo: int, bar: str = resource()) -> None:
            bar.lower()

        with pytest.raises(AttributeError) as exc:
            injected(1)

        exc.match(
            r"Attempted to access an attribute in a resource\(\) marker – did you "
            r"forget to add the @inject decorator\?"
        )

    def test_posonly_argument(self):
        def injected(foo: int, bar: str = resource(), /):
            pass

        pytest.raises(TypeError, inject, injected).match(
            "Cannot inject dependency to positional-only parameter 'bar'"
        )

    def test_no_resources_declared(self) -> None:
        def injected(foo: int) -> None:
            pass

        match = (
            f"{__name__}.{self.__class__.__name__}.test_no_resources_declared.<locals>"
            f".injected does not have any injectable resources declared"
        )
        with pytest.warns(UserWarning, match=match):
            func = inject(injected)

        assert func is injected
