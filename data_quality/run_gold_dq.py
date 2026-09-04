import subprocess
import sys


CHECKS = [
    "check_gold_not_empty.py",
    "check_gold_row_count.py",
    "check_gold_schema.py",
    "check_gold_dates.py",
    "check_gold_duplicates.py",
    "check_gold_nulls.py",
    "check_gold_quantity.py",
    "check_gold_sales_amount.py",
]


def main():
    print("=" * 70)
    print("GOLD DATA QUALITY VALIDATION")
    print("=" * 70)

    for check in CHECKS:
        print("\n" + "-" * 70)
        print(f"Running: {check}")
        print("-" * 70)

        result = subprocess.run(
            [
                "spark-submit",
                f"/home/iceberg/data_quality/checks/{check}",
            ],
            check=False,
        )

        if result.returncode != 0:
            print("\n" + "=" * 70)
            print(f"DATA QUALITY FAILED: {check}")
            print("=" * 70)
            sys.exit(result.returncode)

        print(f"{check} PASSED")

    print("\n" + "=" * 70)
    print("ALL 8 DATA QUALITY CHECKS PASSED")
    print("=" * 70)


if __name__ == "__main__":
    main()