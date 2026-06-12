"""
CUI (Código Único de Identificación) generation utilities.

The CUI is a unique identifier used to cross-reference documents between
different books and records (e.g. SIRE ↔ XML invoices).
It encodes: RUC emisor (hex) + tipo_documento (2 digits) + numero (sanitized).
"""
from typing import Optional


def build_cui_comprobante(
    ruc_emisor: Optional[str],
    tipo_documento: Optional[str],
    numero: Optional[str],
) -> Optional[str]:
    """
    Generate CUI for UBL 2.1 electronic documents (Invoice, Credit Note, etc.).

    Format: {ruc_hex}{tipo_doc:02d}{numero_sin_guion}

    Args:
        ruc_emisor: 11-digit RUC of the issuer
        tipo_documento: Invoice type code (e.g. '01', '07', '08')
        numero: Full document number with serie (e.g. 'F001-00000001')

    Returns:
        CUI string or None if any required field is missing/invalid.
    """
    if not ruc_emisor or not tipo_documento or not numero:
        return None

    try:
        ruc_hex = hex(int(ruc_emisor))[2:].lower()
        tipo_int = int(tipo_documento)
        numero_clean = numero.replace("-", "")
        return f"{ruc_hex}{tipo_int:02d}{numero_clean}"
    except (ValueError, TypeError):
        return None


def build_cui_sire(
    ruc_empresa: Optional[str],
    tipo_comprobante: Optional[str],
    serie: Optional[str],
    numero: Optional[str],
) -> Optional[str]:
    """
    Generate CUI for SIRE records (Ventas/Compras).

    Format: {ruc_hex}{tipo_doc:02d}{serie}{numero}

    For type 53 (liquidación de compra) the RUC comes from the empresa field.
    For all other types, the RUC comes from the document issuer/provider.

    Args:
        ruc_empresa: RUC of the company (used for type 53)
        tipo_comprobante: Document type code (e.g. '01', '07', '53')
        serie: Document series (e.g. 'F001')
        numero: Document correlative number

    Returns:
        CUI string or None if required fields are missing/invalid.
    """
    if tipo_comprobante is None or serie is None or numero is None:
        return None

    try:
        tipo_str = str(tipo_comprobante).strip()
        serie_clean = str(serie).strip()
        numero_clean = str(numero).strip()

        # For liquidation purchases (type 53), use empresa RUC; otherwise use provider RUC
        if tipo_str == "53":
            if ruc_empresa is None:
                return None
            ruc_hex = hex(int(ruc_empresa))[2:].lower()
        else:
            # For SIRE compras, the ruc_empresa field is the provider; for ventas, it's the issuer
            if ruc_empresa is None:
                return None
            ruc_hex = hex(int(ruc_empresa))[2:].lower()

        return f"{ruc_hex}{int(float(tipo_str)):02d}{serie_clean}{numero_clean}"
    except (ValueError, TypeError):
        return None


def build_cui_from_row(row: dict, system: str = "comprobante") -> Optional[str]:
    """
    Generic CUI builder that selects the correct strategy based on system type.

    Args:
        row: A dictionary-like row with keys (ruc_emisor, tipo_documento, etc.)
        system: 'comprobante' for UBL XML, 'sire' for SIRE records

    Returns:
        CUI string or None.
    """
    if system == "comprobante":
        return build_cui_comprobante(
            row.get("ruc_emisor"),
            row.get("tipo_documento"),
            row.get("numero"),
        )
    elif system == "sire":
        return build_cui_sire(
            row.get("ruc"),
            row.get("tipo_comprobante"),
            row.get("numero_serie"),
            row.get("numero_correlativo"),
        )
    return None