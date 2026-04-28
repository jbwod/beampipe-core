"""
PYTHONPATH=src uv run python -m app.core.orchestration.slurm_client.manual_ssh --host setonix.pawsey.org.au --username jblackwood --ssh-key ~/.ssh/pawsey_ed25519_key --known-hosts ~/.ssh/known_hosts --command "hostname && whoami" --passphrase
"""
import argparse
import asyncio
import getpass
import os
from typing import Any

import asyncssh


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument(
        "--username",
        default=os.environ.get("USER"),
    )
    parser.add_argument("--port", type=int, default=22)
    parser.add_argument(
        "--ssh-key",
        action="append",
        default=[],
    )
    parser.add_argument(
        "--known-hosts",
        default="none",
    )
    parser.add_argument(
        "--connect-timeout",
        type=float,
        default=30.0,
    )
    parser.add_argument(
        "--command",
        required=True,
    )
    parser.add_argument(
        "--passphrase",
        default=None,
    )
    parser.add_argument(
        "--passphrase-prompt",
        action="store_true",
    )
    return parser


def _resolve_passphrase(args: argparse.Namespace) -> str | None:
    if args.passphrase_prompt:
        return getpass.getpass("SSH key passphrase: ")
    if args.passphrase:
        return str(args.passphrase)
    return os.environ.get("SSH_KEY_PASSPHRASE")


def _resolved_known_hosts(value: str | None) -> Any:
    if value is None:
        return None
    if value.strip().lower() == "none":
        return None
    return value


async def _run(args: argparse.Namespace) -> int:
    if not args.username:
        raise SystemExit("username is required (pass --username or set $USER)")

    connect_kwargs: dict[str, Any] = {
        "host": args.host,
        "port": args.port,
        "username": args.username,
        "connect_timeout": args.connect_timeout,
        "known_hosts": _resolved_known_hosts(args.known_hosts),
    }
    if args.ssh_key:
        connect_kwargs["client_keys"] = list(args.ssh_key)
    passphrase = _resolve_passphrase(args)
    if passphrase:
        connect_kwargs["passphrase"] = passphrase

    print(f"connecting host={args.host} user={args.username} port={args.port}")
    async with asyncssh.connect(**connect_kwargs) as conn:
        result = await conn.run(args.command, check=False)

    print(f"exit_code={int(result.exit_status or 0)}")
    stdout = result.stdout if isinstance(result.stdout, str) else ""
    stderr = result.stderr if isinstance(result.stderr, str) else ""
    if stdout:
        print("--- stdout ---")
        print(stdout.rstrip("\n"))
    if stderr:
        print("--- stderr ---")
        print(stderr.rstrip("\n"))
    return int(result.exit_status or 0)


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    code = asyncio.run(_run(args))
    raise SystemExit(code)


if __name__ == "__main__":
    main()
