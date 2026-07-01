"""
Central registry mapping document types to their processor classes.

This module bridges three concerns:
1. DOCUMENT_RULES keys (from document_rules.py) → Processor classes
2. API tipo_archivo values (from schemas.py) → DOCUMENT_RULES keys
3. Provides factory methods for pipeline orchestration.
"""
import logging
from typing import Dict, Optional, Type

from src.core.document_rules import DOCUMENT_RULES
from src.processors.base_processor import BaseDocumentProcessor
from src.processors.factura_processor import FacturaProcessor
from src.processors.boleta_venta_processor import BoletaVentaProcessor
from src.processors.nota_credito_processor import NotaCreditoProcessor
from src.processors.nota_debito_processor import NotaDebitoProcessor
from src.processors.guia_remision_processor import GuiaRemisionProcessor
from src.processors.sire_compras_processor import SireComprasProcessor
from src.processors.sire_ventas_processor import SireVentasProcessor
from src.processors.planilla_processor import PlanillaProcessor
from src.processors.recibo_processor import ReciboProcessor
from src.processors.declaracion_pago_processor import DeclaracionPagoProcessor
from src.processors.formulario0621_processor import Formulario0621


# ── DOCUMENT_RULES key → Processor class ──────────────────────────────────

PROCESSOR_MAP: Dict[str, Type[BaseDocumentProcessor]] = {
    "factura_xml": FacturaProcessor,
    "boleta_xml": BoletaVentaProcessor,
    "credito_xml": NotaCreditoProcessor,
    "debito_xml": NotaDebitoProcessor,
    "guia_remision_xml": GuiaRemisionProcessor,
    "sire_compras": SireComprasProcessor,
    "sire_ventas": SireVentasProcessor,
    "reporte_planilla_zip": PlanillaProcessor,
    "recibo_xml": ReciboProcessor,
    "declaraciones_pagos": DeclaracionPagoProcessor,
    "formulario_0621": Formulario0621,
}

# ── API tipo_archivo value → DOCUMENT_RULES key ───────────────────────────

TIPO_ARCHIVO_MAP: Dict[str, str] = {
    "factura": "factura_xml",
    "boleta": "boleta_xml",
    "nota_credito": "credito_xml",
    "nota_debito": "debito_xml",
    "guia_remision": "guia_remision_xml",
    "sire_compras": "sire_compras",
    "sire_ventas": "sire_ventas",
    "planilla": "reporte_planilla_zip",
    "formulario_0621": "formulario_0621",
}


# ── Factory helpers ────────────────────────────────────────────────────────

def get_processor(doc_type: str) -> Optional[Type[BaseDocumentProcessor]]:
    """
    Get the processor class for a given document type key.

    Args:
        doc_type: A key from DOCUMENT_RULES (e.g. 'factura_xml', 'sire_compras')

    Returns:
        The processor class, or None if the doc_type has no processor registered.
    """
    return PROCESSOR_MAP.get(doc_type)


def get_rule_keys_for_tipo(tipo_archivo: str) -> Optional[str]:
    """
    Map an API tipo_archivo value to a DOCUMENT_RULES key.

    Args:
        tipo_archivo: Value from API ParseFilters (e.g. 'factura', 'sire_ventas')

    Returns:
        The corresponding DOCUMENT_RULES key, or None if not found.
    """
    return TIPO_ARCHIVO_MAP.get(tipo_archivo)


def has_processor(doc_type: str) -> bool:
    """
    Check if a document type has a real (non-stub) processor.

    Stub processors exist for document types that are recognized
    but not yet supported for parsing (e.g. recibo_xml, declaraciones_pagos).

    Args:
        doc_type: A key from DOCUMENT_RULES

    Returns:
        True if the type has a real processor, False if stub or missing.
    """
    processor_cls = PROCESSOR_MAP.get(doc_type)
    if processor_cls is None:
        return False
    # Stubs are expected to have a class attribute to identify them
    return getattr(processor_cls, '_is_stub', False) is False