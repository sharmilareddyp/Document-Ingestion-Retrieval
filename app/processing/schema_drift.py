"""Schema drift detection for structured source records.

Breaking drift quarantines the record. Additional optional fields are informational
and do not stop ingestion. The detector compares an expected contract, it does not
infer a schema from the first payload it sees.
"""

from __future__ import annotations

from typing import Any

from app.domain.enums import DriftSeverity
from app.domain.models import DriftFinding
from app.processing.normalization import is_timestamp

_JSON_TYPES = {"string", "integer", "number", "boolean", "object", "array", "timestamp"}


def _actual_type(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    if value is None:
        return "null"
    return type(value).__name__


def _compatible(expected: str, value: Any) -> bool:
    actual = _actual_type(value)
    if expected == "number" and actual in {"integer", "number"}:
        return True
    if expected == "timestamp":
        return actual in {"string", "integer", "number"} and is_timestamp(value)
    if expected == "integer" and actual == "boolean":
        return False
    return actual == expected


def detect_drift(payload: Any, contract: dict[str, Any]) -> list[DriftFinding]:
    """Return drift findings for one record. An empty list means the contract holds."""
    if not isinstance(payload, dict):
        return [
            DriftFinding(
                severity=DriftSeverity.BREAKING.value,
                drift_type="malformed_record",
                field_path="$",
                expected="object",
                actual=_actual_type(payload),
                message="Structured record was not a JSON object",
            )
        ]
    findings: list[DriftFinding] = []
    fields: dict[str, Any] = contract.get("fields") or {}
    additional = contract.get("additional_properties", "informational")
    _walk(payload, fields, additional, "$", findings)
    return findings


def _walk(
    payload: dict[str, Any],
    fields: dict[str, Any],
    additional: str,
    prefix: str,
    findings: list[DriftFinding],
) -> None:
    for name, spec in fields.items():
        path = f"{prefix}.{name}"
        required = bool(spec.get("required", False))
        nullable = bool(spec.get("nullable", not required))
        expected = spec.get("type", "string")
        if expected not in _JSON_TYPES:
            findings.append(
                DriftFinding(
                    severity=DriftSeverity.WARNING.value,
                    drift_type="unknown_contract_type",
                    field_path=path,
                    expected=expected,
                    actual="unrecognized contract type",
                    message="Contract contains an unsupported type",
                )
            )
            continue
        if name not in payload:
            if required:
                findings.append(
                    DriftFinding(
                        severity=DriftSeverity.BREAKING.value,
                        drift_type="missing_required_field",
                        field_path=path,
                        expected=expected,
                        actual="missing",
                        message=f"Required field {path} disappeared",
                    )
                )
            continue
        value = payload[name]
        if value is None:
            severity = (
                DriftSeverity.BREAKING.value
                if (required or not nullable)
                else DriftSeverity.INFORMATIONAL.value
            )
            if required or not nullable:
                findings.append(
                    DriftFinding(
                        severity=severity,
                        drift_type="nullability_change",
                        field_path=path,
                        expected="non-null " + expected,
                        actual="null",
                        message=f"Field {path} is null but the contract does not allow it",
                    )
                )
            continue
        actual = _actual_type(value)
        if expected == "object" and actual == "array":
            findings.append(
                DriftFinding(
                    severity=DriftSeverity.BREAKING.value,
                    drift_type="structure_change",
                    field_path=path,
                    expected="object",
                    actual="array",
                    message=f"Field {path} changed from object to array",
                )
            )
            continue
        if expected == "array" and actual == "object":
            findings.append(
                DriftFinding(
                    severity=DriftSeverity.BREAKING.value,
                    drift_type="structure_change",
                    field_path=path,
                    expected="array",
                    actual="object",
                    message=f"Field {path} changed from array to object",
                )
            )
            continue
        if not _compatible(expected, value):
            drift_type = "timestamp_format" if expected == "timestamp" else "type_change"
            findings.append(
                DriftFinding(
                    severity=DriftSeverity.BREAKING.value,
                    drift_type=drift_type,
                    field_path=path,
                    expected=expected,
                    actual=actual,
                    message=f"Field {path} changed from {expected} to {actual}",
                )
            )
            continue
        if expected == "object" and isinstance(value, dict):
            _walk(value, spec.get("fields") or {}, additional, path, findings)
        if expected == "array" and isinstance(value, list):
            item_spec = spec.get("items") or {"type": "string"}
            item_type = item_spec.get("type", "string")
            for index, item in enumerate(value):
                if item is None:
                    continue
                if not _compatible(item_type, item):
                    findings.append(
                        DriftFinding(
                            severity=DriftSeverity.BREAKING.value,
                            drift_type="type_change",
                            field_path=f"{path}[{index}]",
                            expected=item_type,
                            actual=_actual_type(item),
                            message=f"Array item {path}[{index}] has an unexpected type",
                        )
                    )
    if additional == "allow":
        return
    severity = {
        "informational": DriftSeverity.INFORMATIONAL.value,
        "warning": DriftSeverity.WARNING.value,
        "breaking": DriftSeverity.BREAKING.value,
    }.get(additional, DriftSeverity.INFORMATIONAL.value)
    for name in payload:
        if name not in fields:
            findings.append(
                DriftFinding(
                    severity=severity,
                    drift_type="unexpected_field",
                    field_path=f"{prefix}.{name}",
                    expected="field not in contract",
                    actual=_actual_type(payload[name]),
                    message=f"Payload introduced unexpected field {prefix}.{name}",
                )
            )


def has_breaking(findings: list[DriftFinding]) -> bool:
    return any(finding.severity == DriftSeverity.BREAKING.value for finding in findings)


SAMPLE_RECORDS_CONTRACT: dict[str, Any] = {
    "fields": {
        "id": {"type": "string", "required": True},
        "title": {"type": "string", "required": True},
        "body": {"type": "string", "required": True},
        "updated_at": {"type": "timestamp", "required": True},
        "priority": {"type": "integer", "required": False, "nullable": True},
        "tags": {"type": "array", "required": False, "items": {"type": "string"}},
        "owner": {
            "type": "object",
            "required": False,
            "nullable": True,
            "fields": {"name": {"type": "string", "required": True}},
        },
    },
    "additional_properties": "informational",
}
