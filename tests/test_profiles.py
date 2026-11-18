from pathlib import Path

import pytest
import yaml

from vac.profiles import ProfileError, load_product, load_vendor

ROOT = Path(__file__).resolve().parent.parent
VENDOR = ROOT / "profiles/vendors/smallest.yaml"
PRODUCT = ROOT / "profiles/products/example.yaml"


def _write(tmp_path, data):
    p = tmp_path / "p.yaml"
    p.write_text(yaml.safe_dump(data))
    return p


def test_shipped_profiles_load():
    vendor = load_vendor(VENDOR)
    assert vendor["claims"]["tts.text.max_chars"]["value"] == 8000
    product = load_product(PRODUCT)
    assert product["transport"] == "ws"


def test_vendor_claim_needs_source(tmp_path):
    data = yaml.safe_load(VENDOR.read_text())
    del data["claims"]["tts.text.max_chars"]["source"]
    with pytest.raises(ProfileError, match=r"claims\.tts\.text\.max_chars: missing 'source'"):
        load_vendor(_write(tmp_path, data))


def test_vendor_claim_needs_value_or_default(tmp_path):
    data = yaml.safe_load(VENDOR.read_text())
    data["claims"]["x"] = {"source": "https://example.com"}
    with pytest.raises(ProfileError, match="needs 'value' or 'default'"):
        load_vendor(_write(tmp_path, data))


def test_vendor_missing_auth_env(tmp_path):
    data = yaml.safe_load(VENDOR.read_text())
    del data["auth"]["env"]
    with pytest.raises(ProfileError, match=r"auth: missing 'env'"):
        load_vendor(_write(tmp_path, data))


@pytest.mark.parametrize("path, value, match", [
    (("transport",), "grpc", "transport: must be one of"),
    (("slo", "success_rate"), 1.5, r"success_rate: must be in"),
    (("inputs", "typical"), "   ", "typical: must not be blank"),
    (("sample_rate",), "24000", "sample_rate: expected"),
    (("budget", "max_chars_total"), True, "max_chars_total: expected"),
])
def test_product_validation(tmp_path, path, value, match):
    data = yaml.safe_load(PRODUCT.read_text())
    target = data
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    with pytest.raises(ProfileError, match=match):
        load_product(_write(tmp_path, data))


def test_top_level_must_be_mapping(tmp_path):
    p = tmp_path / "p.yaml"
    p.write_text("- a\n- b\n")
    with pytest.raises(ProfileError, match="top level must be a mapping"):
        load_product(p)
