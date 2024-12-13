import re
from typing import Any, Callable

import duckdb
import jinja2
import requests
from pyarrow import fs


def _strip_prefix_config(prefix: str, config: dict[str, Any]) -> dict[str, Any]:
    return {k[len(prefix):]: v for k, v in config.items() if k.startswith(prefix)}


def default_retriver(location: str, config: dict[str, Any]) -> str:
    return ""


def http_retriever(location: str, config: dict[str, Any]) -> str:
    config = _strip_prefix_config("http_", config)
    method = config.get("method", "GET")
    headers = config.get("headers", {})
    params = config.get("params", {})

    response = requests.request(method, location, headers=headers, params=params)
    response.raise_for_status()

    return response.text


def s3_retriever(s3_path: str, config: dict[str, Any]) -> str:
    config = _strip_prefix_config("s3_", config)
    s3fs = fs.S3FileSystem(**config)

    re_match = re.match(r"s3.?://(.*)", s3_path)
    if not re_match:
        raise ValueError(f"Preload scripts: retrieving an invalid s3 path '{s3_path}'")
    s3_subpath, *_ = re_match.groups()
    with s3fs.open_input_stream(s3_subpath) as f:
        result = f.readall()
    result = result.decode()
    return result


_retriever_registy: dict[str, Callable[[str, dict[str, Any]], str]] = {
    "s3": s3_retriever,
    "s3a": s3_retriever,
    "http": http_retriever,
    "https": http_retriever,
}


def apply_preload_scripts(
    conn: duckdb.DuckDBPyConnection, config: dict[str, Any]
) -> None:
    if not config: # Intentionally do nothing if config empty
        return
    
    location = config.get("location")
    locations = config.get("locations", [])
    parameters = config.get("parameters", {})
    location_config = config.get("config", {})

    if location:
        locations += [location]

    preload_scripts = []
    for location in locations:
        retriever = _retriever_for_location(location)
        preload_tmpl = retriever(location, location_config)
        preload_rendered = _render_preload_script(preload_tmpl, parameters)
        preload_scripts.append(preload_rendered)
    preload_content = "\n\n".join(preload_scripts)

    conn.execute(preload_content)


def _retriever_for_location(location: str) -> Callable[[str, dict[str, Any]], str]:
    re_match = re.match("^([^:]+)://.*", location)
    if not re_match:
        raise ValueError(f"Preload scripts: invalid location value '{location}'")
    protocol, *_ = re_match.groups()
    retriever = _retriever_registy.get(protocol, default_retriver)
    return retriever


def _render_preload_script(script_tmpl: str, parameters: dict[str, Any]) -> str:
    return jinja2.Template(script_tmpl).render(parameters)
