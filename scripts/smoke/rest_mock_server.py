#!/usr/bin/env python3

from http.server import BaseHTTPRequestHandler, HTTPServer
import argparse
import json


class ServerState:
    def __init__(self, count: int, id_prefix: str, line_prefix: str):
        self.count = count
        self.id_prefix = id_prefix
        self.line_prefix = line_prefix


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/events":
            self.send_response(404)
            self.end_headers()
            return

        state: ServerState = self.server.state  # type: ignore[attr-defined]
        body = {
            "items": [
                {
                    "event_id": f"{state.id_prefix}-{index:04d}",
                    "status": "ok" if index % 2 == 0 else "warn",
                    "line": f"{state.line_prefix}{(index % 20) + 1}",
                }
                for index in range(1, state.count + 1)
            ]
        }
        encoded = json.dumps(body).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format, *args):
        return


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--count", type=int, default=200)
    parser.add_argument("--id-prefix", default="rest")
    parser.add_argument("--line-prefix", default="L")
    args = parser.parse_args()

    if args.count <= 0:
        raise SystemExit("--count must be > 0")

    server = HTTPServer((args.host, args.port), Handler)
    server.state = ServerState(args.count, args.id_prefix, args.line_prefix)  # type: ignore[attr-defined]
    server.serve_forever()


if __name__ == "__main__":
    main()
