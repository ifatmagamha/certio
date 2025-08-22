import pandas as pd
import json
from pathlib import Path

def guess_param_key_and_unit(unit_type, unit):
    """
    Identify param_key, param_unit based on the unit_type + unit.
    """
    if pd.isna(unit_type) or pd.isna(unit):
        return None

    unit_type = unit_type.strip().lower()
    unit = unit.strip().lower()

    if "room-night" in unit or "night" in unit:
        return ("number", 1, None)

    if "money" in unit_type or "currency" in unit_type or "/usd" in unit:
        return ("money", 1000, "usd")

    if "mass" in unit_type or unit.endswith("kg") or unit.endswith("t"):
        return ("weight", 80, unit.split("/")[-1])  

    if "energy" in unit_type or "kwh" in unit:
        return ("energy", 1000, "kWh")

    if "distance" in unit_type or "km" in unit:
        return ("distance", 100, "km")

    if "number" in unit_type and "/" not in unit:
        return ("number", 1, None)

    return ("unknown_param", 1, None)


def generate_parameter_rules(search_csv_path: Path, output_json_path: Path):
    df = pd.read_csv(search_csv_path)

    rules = {}
    for _, row in df.iterrows():
        unit = str(row.get("unit")).strip()
        unit_type = str(row.get("unit_type")).strip()

        if not unit or unit in rules:
            continue

        param_key, param_value, param_unit = guess_param_key_and_unit(unit_type, unit)
        rules[unit] = (param_key, param_value, param_unit)

    output_json_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_json_path, "w", encoding="utf-8") as f:
        json.dump(rules, f, indent=2)
    print(f"[INFO] PARAMETER_RULES generated and saved to {output_json_path}")


if __name__ == "__main__":
    generate_parameter_rules(
    search_csv_path=Path("data/raw/climatiq_search_FR.csv"),
    output_json_path=Path("data/config/generated_parameter_rules_FR.json")
    )
