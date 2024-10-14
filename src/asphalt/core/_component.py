from __future__ import annotations

import logging
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from collections.abc import (
    Awaitable,
    Coroutine,
    Mapping,
    MutableMapping,
    Sequence,
)
from dataclasses import dataclass
from inspect import isclass
from traceback import StackSummary
from types import FrameType
from typing import (
    Any,
    Callable,
    ClassVar,
    Literal,
    TypeVar,
    get_type_hints,
    overload,
)

from anyio import (
    create_task_group,
    move_on_after,
)
from anyio.abc import TaskStatus

from ._context import (
    Context,
    FactoryCallback,
    T_Resource,
    TeardownCallback,
    current_context,
)
from ._event import Event, Signal, wait_event
from ._exceptions import ComponentStartError, NoCurrentContext, ResourceNotFound
from ._utils import (
    PluginContainer,
    coalesce_exceptions,
    format_component_name,
    merge_config,
    qualified_name,
)

logger = logging.getLogger("asphalt.core")

TComponent = TypeVar("TComponent", bound="Component")


class Component(metaclass=ABCMeta):
    """This is the base class for all Asphalt components."""

    _isolated: ClassVar[bool]

    _child_components: dict[str, dict[str, Any]] | None = None
    _component_started = False

    def __init_subclass__(cls, *, isolated: bool = False, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls._isolated = isolated

    def add_component(
        self, alias: str, /, type: str | type[Component] | None = None, **config: Any
    ) -> None:
        """
        Add a child component.

        This will store the type and configuration options of the named child component,
        to be later instantiated by :func:`start_component`.

        If the ``type`` argument is omitted, then the value of the ``alias`` argument is
        used to derive the type.

        The locally given configuration can be overridden by component configuration
        parameters supplied to the constructor (via the ``components`` argument).

        When configuration values are provided both as keyword arguments to this method
        and component configuration through the ``components`` constructor argument, the
        configurations are merged together using :func:`~asphalt.core.merge_config`
        in a way that the configuration values from the ``components`` argument override
        the keyword arguments to this method.

        :param alias: a name for the component instance, unique within this container
        :param type: name of and entry point in the ``asphalt.components`` namespace or
            a :class:`Component` subclass
        :param config: mapping of keyword arguments passed to the component's
            initializer
        :raises RuntimeError: if there is already a child component with the same alias

        """
        if self._component_started:
            raise RuntimeError(
                "child components cannot be added once start_component() has been "
                "called on the component"
            )

        if not isinstance(alias, str) or not alias:
            raise TypeError("alias must be a nonempty string")

        if self._child_components is None:
            self._child_components = {}
        elif alias in self._child_components:
            raise ValueError(f'there is already a child component named "{alias}"')

        self._child_components[alias] = {"type": type or alias, **config}

    async def prepare(self) -> None:
        """
        Perform any necessary initialization before starting the component.

        This method is called by :func:`start_component` *before* starting the child
        components of this component, so it can be used to add any resources required
        by the child components.
        """

    async def start(self) -> None:
        """
        Perform any necessary tasks to start the services provided by this component.

        This method is called by :func:`start_component` *after* the child components of
        this component have been started, so any resources provided by the child
        components are available at this point.

        .. warning:: Do not call this method directly; use :func:`start_component`
            instead.
        """


class CLIApplicationComponent(Component):
    """
    Specialized :class:`.Component` subclass for command line tools.

    Command line tools and similar applications should use this as their root component
    and implement their main code in the :meth:`run` method.

    When all the subcomponents have been started, :meth:`run` is started as a new task.
    When the task is finished, the application will exit using the return value as its
    exit code.

    If :meth:`run` raises an exception, a stack trace is printed and the exit code will
    be set to 1. If the returned exit code is out of range or of the wrong data type,
    it is set to 1 and a warning is emitted.
    """

    @abstractmethod
    async def run(self) -> int | None:
        """
        Run the business logic of the command line tool.

        Do not call this method yourself.

        :return: the application's exit code (0-127; ``None`` = 0)
        """


component_types = PluginContainer("asphalt.components", Component)


@overload
async def start_component(
    component_class: type[TComponent],
    config: dict[str, Any] | None = ...,
    *,
    timeout: float | None = ...,
) -> TComponent: ...


@overload
async def start_component(
    component_class: str,
    config: dict[str, Any] | None = ...,
    *,
    timeout: float | None = ...,
) -> Component: ...


async def start_component(
    component_class: type[Component] | str,
    config: dict[str, Any] | None = None,
    *,
    timeout: float | None = 20,
) -> Component:
    """
    Start a component and its subcomponents.

    :param component_class: the root component class, an entry point name in the
        ``asphalt.components`` namespace or a ``modulename:varname`` reference
    :param config: configuration for the root component (and its child components)
    :param timeout: seconds to wait for all the components in the hierarchy to start
        (default: ``20``; set to ``None`` to disable timeout)
    :raises RuntimeError: if this function is called without an active :class:`Context`
    :raises TimeoutError: if the startup of the component hierarchy takes more than
        ``timeout`` seconds
    :raises TypeError: if ``component_class`` is neither a :class:`Component` subclass
        or a string

    """
    try:
        current_context()
    except NoCurrentContext:
        raise RuntimeError(
            "start_component() requires an active Asphalt context"
        ) from None

    if config is None:
        config = {}

    orchestrator = ComponentStartupOrchestrator(component_class, config)
    return await orchestrator.start_component_tree(timeout)


class ComponentStartupEvent(Event):
    __slots__ = "component_class", "path", "status", "coro"

    def __init__(
        self,
        component_class: type[Component],
        path: str,
        status: str,
        coro: Coroutine[Any, Any, Any] | None = None,
    ):
        self.component_class = component_class
        self.path = path
        self.status = status
        self.coro = coro


class ComponentContextProxy(Context):
    _parent: Context

    def __init__(
        self,
        path: str,
        component_class: type[Component],
        orchestrator: ComponentStartupOrchestrator,
    ):
        super().__init__()
        self.__path = path
        self.__component_class = component_class
        self.__orchestrator = orchestrator

        # Proxy the real context, not another component proxy
        while isinstance(self._parent, ComponentContextProxy):
            self._parent = self._parent._parent

    def __format_resource_description(
        self, types: Any, name: str, description: str | None = None
    ) -> str:
        if isclass(types):
            formatted = f"type={qualified_name(types)}"
        else:
            formatted_types = [qualified_name(type_) for type_ in types]
            formatted = f"types={formatted_types}"

        formatted += f", name={name!r}"
        if description:
            formatted += f", description={description!r}"

        return formatted

    def add_resource(
        self,
        value: T_Resource,
        name: str = "default",
        types: type | Sequence[type] = (),
        *,
        description: str | None = None,
        teardown_callback: Callable[[], Any] | None = None,
    ) -> None:
        self._parent.add_resource(
            value,
            name,
            types,
            description=description,
            teardown_callback=teardown_callback,
        )
        logger.debug(
            "%s added a resource (%s)",
            format_component_name(self.__path, capitalize=True),
            self.__format_resource_description(types or type(value), name, description),
        )

    def add_resource_factory(
        self,
        factory_callback: FactoryCallback,
        name: str = "default",
        *,
        types: Sequence[type] | None = None,
        description: str | None = None,
    ) -> None:
        self._parent.add_resource_factory(
            factory_callback, name, types=types, description=description
        )
        logger.debug(
            "%s added a resource factory (%s)",
            format_component_name(self.__path, capitalize=True),
            self.__format_resource_description(
                types or get_type_hints(factory_callback)["return"], name, description
            ),
        )

    @overload
    async def get_resource(
        self,
        type: type[T_Resource],
        name: str = ...,
        *,
        optional: Literal[True],
    ) -> T_Resource | None: ...

    @overload
    async def get_resource(
        self,
        type: type[T_Resource],
        name: str = ...,
        *,
        optional: Literal[False],
    ) -> T_Resource: ...

    @overload
    async def get_resource(
        self, type: type[T_Resource], name: str = ...
    ) -> T_Resource: ...

    async def get_resource(
        self,
        type: type[T_Resource],
        name: str = "default",
        *,
        optional: Literal[False, True] = False,
    ) -> T_Resource | None:
        if optional:
            return await self._parent.get_resource(type, name, optional=True)

        try:
            return await self._parent.get_resource(type, name)
        except ResourceNotFound:
            logger.debug(
                "%s is waiting for another component to provide a resource (%s)",
                format_component_name(self.__path, capitalize=True),
                self.__format_resource_description(type, name),
            )

            # Wait until a matching resource or resource factory is available
            signals = [ctx.resource_added for ctx in self._parent.context_chain]
            await wait_event(
                signals,
                lambda event: event.resource_name == name
                and type in event.resource_types,
            )
            res = await self.get_resource(type, name)
            logger.debug(
                "%s got the resource it was waiting for (%s)",
                format_component_name(self.__path, capitalize=True),
                self.__format_resource_description(type, name),
            )
            return res

    @overload
    def get_resource_nowait(
        self, type: type[T_Resource], name: str = ..., *, optional: Literal[True]
    ) -> T_Resource | None: ...

    @overload
    def get_resource_nowait(
        self, type: type[T_Resource], name: str = ..., *, optional: Literal[False]
    ) -> T_Resource: ...

    @overload
    def get_resource_nowait(
        self, type: type[T_Resource], name: str = ...
    ) -> T_Resource: ...

    def get_resource_nowait(
        self,
        type: type[T_Resource],
        name: str = "default",
        *,
        optional: Literal[False, True] = False,
    ) -> T_Resource | None:
        if optional:
            return self._parent.get_resource_nowait(type, name, optional=True)

        return self._parent.get_resource_nowait(type, name)

    def get_resources(self, type: type[T_Resource]) -> Mapping[str, T_Resource]:
        return self._parent.get_resources(type)

    def add_teardown_callback(
        self, callback: TeardownCallback, pass_exception: bool = False
    ) -> None:
        self._parent.add_teardown_callback(callback, pass_exception)


class ComponentStartupOrchestrator:
    component_status_changed = Signal(ComponentStartupEvent)

    _components_by_path: dict[str, Component]
    _child_component_aliases: dict[str, set[str]]

    def __init__(
        self, root_component_class: type[Component] | str, config: Mapping[str, Any]
    ):
        self.root_component_class = root_component_class
        self.config = config
        self._components_by_path = {}
        self._child_component_aliases = defaultdict(set)

    def _init_component(self, path: str, config: MutableMapping[str, Any]) -> None:
        if not isinstance(config, MutableMapping):
            raise TypeError(
                f"{path}: component configuration must be either None or a dict (or "
                f"any other mutable mapping type)"
            )

        # Separate the child components from the config
        child_components_config = config.pop("components", {})

        # Resolve the type to a class
        component_type = config.pop("type")
        component_class = component_types.resolve(component_type)
        if not isclass(component_class) or not issubclass(component_class, Component):
            raise TypeError(
                f"{component_type!r} resolved to {component_class} which is not a subclass "
                f"of Component"
            )

        # Instantiate the component
        logger.debug("Creating %s", format_component_name(path, component_class))
        self.component_status_changed.dispatch(
            ComponentStartupEvent(component_class, path, "creating")
        )
        try:
            component = self._components_by_path[path] = component_class(**config)
        except Exception as exc:
            raise ComponentStartError("creating", path, component_class) from exc

        logger.debug("Created %s", format_component_name(path, component_class))
        self.component_status_changed.dispatch(
            ComponentStartupEvent(component_class, path, "created")
        )

        # Merge the overrides to the hard-coded configuration
        child_components_config = merge_config(
            component._child_components, child_components_config
        )
        self._child_component_aliases[path] = set(child_components_config)

        # Create the child components
        for alias, child_config in child_components_config.items():
            if child_config is None:
                child_config = {}

            # If the type was specified only via an alias, use that as a type
            child_config.setdefault("type", alias)

            # If the type contains a forward slash, split the latter part out of it
            if isinstance(child_config["type"], str) and "/" in child_config["type"]:
                child_config["type"] = child_config["type"].split("/")[0]

            final_path = f"{path}.{alias}" if path else alias
            self._init_component(final_path, child_config)

    async def _start_component(self, component: Component, path: str) -> Component:
        # Prevent add_component() from being called beyond this point
        component._component_started = True

        component_class = type(component)
        async with ComponentContextProxy(path, component_class, self):
            # Call prepare() on the component itself, if it's implemented on the component
            # class
            if component_class.prepare is not Component.prepare:
                logger.debug("Calling prepare() of %s", format_component_name(path))
                coro = component.prepare()
                self.component_status_changed.dispatch(
                    ComponentStartupEvent(component_class, path, "preparing", coro)
                )
                try:
                    await coro
                except Exception as exc:
                    raise ComponentStartError(
                        "preparing", path, component_class
                    ) from exc

                logger.debug(
                    "Returned from prepare() of %s", format_component_name(path)
                )

            # Start the child components, if there are any
            if child_component_aliases := self._child_component_aliases.get(path):
                logger.debug(
                    "Starting the child components of %s", format_component_name(path)
                )
                self.component_status_changed.dispatch(
                    ComponentStartupEvent(component_class, path, "starting children")
                )
                async with create_task_group() as tg:
                    for alias in child_component_aliases:
                        final_path = f"{path}.{alias}" if path else alias
                        child_component = self._components_by_path[final_path]
                        tg.start_soon(
                            self._start_component,
                            child_component,
                            final_path,
                            name=(
                                f"Starting component {final_path} "
                                f"({qualified_name(child_component)})"
                            ),
                        )

            # Call start() on the component itself, if it's implemented on the component
            # class
            if component_class.start is not Component.start:
                logger.debug("Calling start() of %s", format_component_name(path))
                coro = component.start()
                self.component_status_changed.dispatch(
                    ComponentStartupEvent(component_class, path, "starting", coro)
                )
                try:
                    await coro
                except Exception as exc:
                    raise ComponentStartError(
                        "starting", path, component_class
                    ) from exc

                logger.debug("Returned from start() of %s", format_component_name(path))

        self.component_status_changed.dispatch(
            ComponentStartupEvent(component_class, path, "started")
        )
        return component

    async def start_component_tree(self, timeout: float | None) -> Component:
        with coalesce_exceptions():
            async with create_task_group() as tg:
                await tg.start(self._watch_component_tree_startup, timeout)
                self._init_component(
                    "", {"type": self.root_component_class, **self.config}
                )
                return await self._start_component(self._components_by_path[""], "")

    async def _watch_component_tree_startup(
        self, timeout: float, *, task_status: TaskStatus[None]
    ) -> None:
        @dataclass
        class ComponentStatus:
            component_class: type[Component]
            status: str
            coro: Coroutine[Any, Any, Any] | None

        component_statuses: dict[str, ComponentStatus] = {}
        async with self.component_status_changed.stream_events(
            max_queue_size=200
        ) as events:
            task_status.started()
            with move_on_after(timeout):
                async for event in events:
                    if event.status == "creating":
                        component_statuses[event.path] = ComponentStatus(
                            event.component_class, event.status, event.coro
                        )
                    elif event.status == "started":
                        del component_statuses[event.path]
                        if not component_statuses:
                            break
                    else:
                        component_statuses[event.path].status = event.status
                        component_statuses[event.path].coro = event.coro

        if component_statuses:
            status_summary_sections: list[str] = [
                "Timeout waiting for the component tree to start"
            ]

            status_summary: list[str] = []
            for path, status in component_statuses.items():
                parts = (path or "(root)").split(".")
                indent = "  " * (len(parts) if path else 0)
                status_summary.append(f"{indent}{parts[-1]}: {status.status}")

            title = "Current status of the components still waiting to finish startup"
            status_summary_sections.append(f"{title}\n{'-' * len(title)}")
            status_summary_sections.append("\n".join(status_summary))

            stack_summaries: list[str] = []
            for path, status in component_statuses.items():
                if status.coro is not None:
                    stack_summary = self._get_coro_stack_summary(status.coro)
                    formatted_summary = "".join(stack_summary.format())
                    title = f"{path} ({qualified_name(status.component_class)})"
                    stack_summaries.append(f"{title}:\n{formatted_summary.rstrip()}")

            if stack_summaries:
                title = "Stack summaries of components still waiting to start"
                status_summary_sections.append(f"{title}\n{'-' * len(title)}")
                status_summary_sections.extend(stack_summaries)

            logger.error("%s", "\n\n".join(status_summary_sections))
            raise TimeoutError("timeout starting component tree")

    @staticmethod
    def _get_coro_stack_summary(coro: Coroutine[Any, Any, Any]) -> StackSummary:
        import gc

        frames: list[FrameType] = []
        awaitable: Awaitable[Any] | None = coro
        while isinstance(awaitable, Coroutine):
            while awaitable.__class__.__name__ == "async_generator_asend":
                # Hack to get past asend() objects
                awaitable = gc.get_referents(awaitable)[0].ag_await

            if frame := getattr(awaitable, "cr_frame", None):
                frames.append(frame)

            awaitable = getattr(awaitable, "cr_await", None)

        frame_tuples = [(f, f.f_lineno) for f in frames]
        return StackSummary.extract(frame_tuples)
