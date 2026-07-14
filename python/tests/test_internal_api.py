import ast
import inspect
from pathlib import Path

import openkeyv
from openkeyv import _internal

STUB_PATH = Path(__file__).parents[1] / "src" / "openkeyv" / "_internal.pyi"


def _stub_signature(node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[tuple[str, bool]]:
    positional = [*node.args.posonlyargs, *node.args.args]
    defaults = [None] * (len(positional) - len(node.args.defaults)) + [*node.args.defaults]
    parameters = [(argument.arg, default is not None) for argument, default in zip(positional, defaults, strict=True)]
    parameters.extend(
        (argument.arg, default is not None) for argument, default in zip(node.args.kwonlyargs, node.args.kw_defaults, strict=True)
    )
    return [(name, optional) for name, optional in parameters if name != "self"]


def _runtime_signature(target: object) -> list[tuple[str, bool]]:
    return [
        (parameter.name, parameter.default is not inspect.Parameter.empty)
        for parameter in inspect.signature(target).parameters.values()
        if parameter.name != "self"
    ]


def test_runtime_api_matches_type_stub() -> None:
    module = ast.parse(STUB_PATH.read_text())
    stub_classes = {node.name: node for node in module.body if isinstance(node, ast.ClassDef)}

    assert set(stub_classes) == set(openkeyv.__all__)

    for class_name in openkeyv.__all__:
        runtime_class = getattr(_internal, class_name)
        stub_class = stub_classes[class_name]
        stub_functions = {node.name: node for node in stub_class.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
        stub_methods = {name for name in stub_functions if name != "__init__"}
        runtime_methods = {name for name, value in runtime_class.__dict__.items() if not name.startswith("_") and callable(value)}

        assert stub_methods == runtime_methods, class_name
        assert _stub_signature(stub_functions["__init__"]) == _runtime_signature(runtime_class), class_name

        for method_name in stub_methods:
            assert _stub_signature(stub_functions[method_name]) == _runtime_signature(getattr(runtime_class, method_name)), (
                class_name,
                method_name,
            )


def test_codec_api_matches_type_stub() -> None:
    module = ast.parse(STUB_PATH.read_text())
    stub_functions = {node.name: node for node in module.body if isinstance(node, ast.FunctionDef)}

    for function_name in ("_encode_entry", "_decode_entry"):
        assert _stub_signature(stub_functions[function_name]) == _runtime_signature(getattr(_internal, function_name))
