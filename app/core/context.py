from sentry_sdk.utils import ContextVar

request_id_context_var: ContextVar[str] = ContextVar('request_id', default=1)
