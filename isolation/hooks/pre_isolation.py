from typing import Dict, Any


class PreIsolationHook:
    @classmethod
    def _set_kwargs_to_env(cls):
        raise NotImplementedError

    @classmethod
    def _set_python_callable_to_env(cls):
        raise NotImplementedError

    @classmethod
    def _set_context_to_env(cls) -> Dict[str, Any]:
        raise NotImplementedError
