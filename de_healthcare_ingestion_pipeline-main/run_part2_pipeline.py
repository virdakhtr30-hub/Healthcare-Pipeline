import subprocess
import sys


def run_step(name: str, command: list[str]):
    print(f"\n=== Running {name} ===")
    result = subprocess.run(command)

    if result.returncode != 0:
        raise RuntimeError(f"{name} failed")

    print(f"=== Completed {name} ===")


if __name__ == "__main__":
    python = sys.executable

    run_step("Bronze Load", [python, "run_part2_bronze.py"])
    run_step("Storage Metrics", [python, "run_part2_storage_metrics.py"])
    run_step("Silver Transformations", [python, "run_part2_silver.py"])
    run_step("Gold Transformations", [python, "run_part2_gold.py"])

    print("\nPart 2 pipeline completed successfully.")