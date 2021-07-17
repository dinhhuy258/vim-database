from dataclasses import dataclass
import subprocess


@dataclass(frozen=True)
class CommandResult:
    error: bool
    data: str


def run_command(command: list, environment: dict = None) -> CommandResult:
    if environment is None:
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    else:
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=environment)
    if result.returncode == 0:
        return CommandResult(error=False, data=result.stdout.rstrip())

    return CommandResult(error=True, data=result.stderr.rstrip())
