"""
Document type identification rules for SUNAT file parsing.
Contains regex patterns that classify files by type and extract metadata.

Each rule is a tuple of (compiled_regex, named_groups_list).
"""
import re
from typing import Pattern, Tuple, List, Optional

# ── Document Recognition Rules ──────────────────────────────────────────────
# Structure: { doc_type: (compiled_regex, [group_names]) }
# The group_names list maps regex groups to semantic field names.
DOCUMENT_RULES: dict[str, Tuple[Pattern, List[str]]] = {
    "declaraciones_pagos": (
        re.compile(r"^DetalleDeclaraciones_(\d{11})_(\d{14})\.(xlsx)$", re.IGNORECASE),
        ["ruc", "timestamp", "ext"],
    ),
    "guia_remision_xml": (
        re.compile(r"^(\d{11})-09-([A-Z0-9]{4})-(\d{1,8})\.(xml)$", re.IGNORECASE),
        ["ruc", "serie", "correlativo", "ext"],
    ),
    "sire_compras": (
        re.compile(r"^(\d{11})-\d{8}-\d{4,6}-propuesta(?:.{0,20})\.(zip|txt)$", re.IGNORECASE),
        ["ruc"],
    ),
    "sire_ventas": (
        re.compile(r"^LE(\d{11})\d{6}1?\d+EXP2(?:.{0,20})\.(zip|txt)$", re.IGNORECASE),
        ["ruc"],
    ),
    "factura_xml": (
        re.compile(r"^FACTURA([A-Z0-9]{4})-?(\d{1,8})(\d{11})\.(zip|xml)$", re.IGNORECASE),
        ["serie", "correlativo", "ruc", "ext"],
    ),
    "boleta_xml": (
        re.compile(r"^BOLETA([A-Z0-9]{4})-(\d{1,8})(\d{11})\.(zip|xml)$", re.IGNORECASE),
        ["serie", "correlativo", "ruc", "ext"],
    ),
    "credito_xml": (
        re.compile(r"^NOTA_CREDITO([A-Z0-9]{4})_?(\d{1,8})(\d{11})\.(zip|xml)$", re.IGNORECASE),
        ["serie", "correlativo", "ruc", "ext"],
    ),
    "debito_xml": (
        re.compile(r"^NOTA_DEBITO([A-Z0-9]{4})_?(\d{1,8})(\d{11})\.(zip|xml)$", re.IGNORECASE),
        ["serie", "correlativo", "ruc", "ext"],
    ),
    "recibo_xml": (
        re.compile(r"^RHE(\d{11})(\d{1,8})\.(xml)$", re.IGNORECASE),
        ["ruc", "correlativo", "ext"],
    ),
    "reporte_planilla_zip": (
        re.compile(r"^(\d{11})_([A-Z]{3})_(\d{8})\.(zip)$", re.IGNORECASE),
        ["ruc", "codigo", "fecha", "ext"],
    ),
    "formulario_0621": (
        re.compile(r"^(\d{11})_0621_(\d{10,15})_(\d+)\.(zip)$", re.IGNORECASE),
        ["ruc", "orden", "id", "ext"],
    ),
}


def identify_document_type(filename: str) -> Optional[str]:
    """
    Match a filename against all known document rules.

    Args:
        filename: The bare filename (e.g. 'FACTURA001-00000123456789012.xml')

    Returns:
        The document type key (e.g. 'factura_xml') or 'desconocido' if no match.
    """
    for doc_type, (pattern, _) in DOCUMENT_RULES.items():
        if pattern.match(filename):
            return doc_type
    return "desconocido"