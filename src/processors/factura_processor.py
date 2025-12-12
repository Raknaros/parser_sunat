from .base_processor import BaseDocumentProcessor
from utils.xml_utils import get_xml_encoding
import pandas as pd
from pathlib import Path
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional
import logging
import zipfile

class FacturaProcessor(BaseDocumentProcessor):
    def __init__(self, logger):
        super().__init__(logger)
        self.NAMESPACES = {
            'cbc': 'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2',
            'cac': 'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2',
            'ext': 'urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2',
            'sac': 'urn:sunat:names:specification:ubl:peru:schema:xsd:SunatAggregateComponents-1'
        }

    def get_db_mapping(self) -> Dict[str, Dict]:
        return {
            'header': {
                'table': 'stg_xml_headers',
                'schema': 'meta',
                'columns': {
                    'CUI': 'cui', 'numero': 'serie_numero', 'fecha_emision': 'fecha_emision',
                    'tipo_documento': 'tipo_documento', 'moneda': 'moneda', 'ruc_emisor': 'ruc_emisor',
                    'nombre_emisor': 'nombre_emisor', 'ruc_receptor': 'ruc_receptor',
                    'documento_receptor':'documento_receptor', 'nombre_receptor': 'nombre_receptor',
                    'importe_total': 'importe_total', 'total_valor_venta':'total_valor_venta',
                    'total_descuentos': 'total_descuentos', 'total_otros_cargos': 'total_otros_cargos',
                    'total_anticipos': 'total_anticipos', 'total_igv': 'total_igv', 'total_isc': 'total_isc',
                    'total_otros_tributos': 'total_otros_tributos', 'total_exonerado': 'total_exonerado',
                    'total_inafecto': 'total_inafecto', 'total_gratuito': 'total_gratuito',
                    'tipo_operacion': 'tipo_operacion',
                    'indicador_retencion': 'retencion',
                    'indicador_detraccion': 'detraccion'
                }
            },
            'lines': {
                'table': 'stg_xml_items',
                'schema': 'meta',
                'columns': {
                    'CUI': 'cui', 'linea_id': 'linea_id', 'cantidad': 'cantidad',
                    'unidad': 'unidad', 'descripcion': 'descripcion', 'codigo_producto': 'codigo_producto',
                    'precio_unitario': 'precio_unitario', 'subtotal': 'subtotal', 'linea_igv': 'linea_igv',
                    'linea_igv_porcentaje': 'linea_igv_porcentaje'
                }
            },
            'payment_terms': {
                'table': 'stg_xml_pagos',
                'schema': 'meta',
                'columns': {
                    'CUI': 'cui', 'forma_pago': 'forma_pago', 'monto_pago': 'monto_pago',
                    'moneda_pago': 'moneda_pago', 'fecha_vencimiento': 'fecha_vencimiento',
                }
            },
            'despatch_references': {
                'table': 'stg_xml_references',
                'schema': 'meta',
                'columns': {
                    'CUI': 'cui', 'guia_numero': 'serie_numero', 'guia_tipo_documento': 'tipo_comprobante'
                }
            }
        }

    def safe_find_text(self, element, xpath, namespaces=None):
        found = element.find(xpath, namespaces)
        return found.text if found is not None else None

    def safe_find_attr(self, element, xpath, attr_name, namespaces=None):
        found = element.find(xpath, namespaces)
        return found.get(attr_name) if found is not None else None

    def process_file(self, file_path: str) -> Optional[Dict[str, pd.DataFrame]]:
        file_name = Path(file_path).name
        self.log_operation("Procesamiento", "Iniciado", f"Archivo: {file_name}")

        try:
            xml_content = None

            # Caso 1: Archivo ZIP
            if file_path.lower().endswith('.zip'):
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    # Buscar el primer archivo .xml dentro del zip
                    xml_filename = next((name for name in zip_ref.namelist() if name.lower().endswith('.xml')), None)

                    if xml_filename:
                        with zip_ref.open(xml_filename) as f:
                            # Leer bytes y decodificar
                            content_bytes = f.read()
                            # Intento simple de detección de encoding o fallback a ISO-8859-1
                            try:
                                xml_content = content_bytes.decode('utf-8')
                            except UnicodeDecodeError:
                                xml_content = content_bytes.decode('ISO-8859-1')
                    else:
                        self.logger.warning(f"El archivo ZIP {file_name} no contiene ningún XML.")
                        return None

            # Caso 2: Archivo XML directo
            else:
                encoding = get_xml_encoding(file_path)
                with open(file_path, 'r', encoding=encoding) as f:
                    xml_content = f.read()

            if xml_content is None:
                return None

            root = ET.fromstring(xml_content)
            
            result = {'header': pd.DataFrame(), 'lines': pd.DataFrame(), 'payment_terms': pd.DataFrame(), 'despatch_references': pd.DataFrame()}
            
            invoice_data = {}
            invoice_data['numero'] = self.safe_find_text(root, './/cbc:ID', self.NAMESPACES)
            invoice_data['fecha_emision'] = self.safe_find_text(root, './/cbc:IssueDate', self.NAMESPACES)
            invoice_data['tipo_documento'] = self.safe_find_text(root, './/cbc:InvoiceTypeCode', self.NAMESPACES)
            invoice_data['moneda'] = self.safe_find_attr(root, './/cbc:DocumentCurrencyCode', 'currencyID', self.NAMESPACES) or self.safe_find_text(root, './/cbc:DocumentCurrencyCode', self.NAMESPACES)

            invoice_data['tipo_operacion'] = self.safe_find_attr(root, './/cbc:InvoiceTypeCode', 'listID', self.NAMESPACES)

            supplier = root.find('.//cac:AccountingSupplierParty', self.NAMESPACES)
            if supplier:
                invoice_data['ruc_emisor'] = self.safe_find_text(supplier, './/cbc:ID', self.NAMESPACES)
                invoice_data['nombre_emisor'] = self.safe_find_text(supplier, './/cac:Party/cac:PartyLegalEntity/cbc:RegistrationName', self.NAMESPACES)

            customer = root.find('.//cac:AccountingCustomerParty', self.NAMESPACES)
            if customer:
                invoice_data['ruc_receptor'] = self.safe_find_text(customer, './/cbc:ID', self.NAMESPACES)
                invoice_data['documento_receptor'] = self.safe_find_attr(customer, './/cbc:ID', 'schemeID',self.NAMESPACES)
                invoice_data['nombre_receptor'] = self.safe_find_text(customer, './/cac:Party/cac:PartyLegalEntity/cbc:RegistrationName', self.NAMESPACES)

            legal_monetary = root.find('.//cac:LegalMonetaryTotal', self.NAMESPACES)
            if legal_monetary:
                invoice_data['importe_total'] = self.safe_find_text(legal_monetary, './/cbc:PayableAmount', self.NAMESPACES)
                invoice_data['total_descuentos'] = self.safe_find_text(legal_monetary, './/cbc:AllowanceTotalAmount', self.NAMESPACES)
                invoice_data['total_otros_cargos'] = self.safe_find_text(legal_monetary, './/cbc:ChargeTotalAmount', self.NAMESPACES)
                invoice_data['total_anticipos'] = self.safe_find_text(legal_monetary, './/cbc:PrepaidAmount', self.NAMESPACES)

            tax_total = root.find('.//cac:TaxTotal', self.NAMESPACES)
            if tax_total:
                invoice_data['total_igv'] = None
                invoice_data['total_isc'] = None
                invoice_data['total_otros_tributos'] = None
                invoice_data['total_exonerado'] = None
                invoice_data['total_inafecto'] = None
                invoice_data['total_gratuito'] = None
                
                for tax_subtotal in tax_total.findall('.//cac:TaxSubtotal', self.NAMESPACES):
                    tax_scheme = tax_subtotal.find('.//cac:TaxCategory/cac:TaxScheme/cbc:ID', self.NAMESPACES)
                    if tax_scheme is not None:
                        tax_code = tax_scheme.text
                        tax_amount = self.safe_find_text(tax_subtotal, './/cbc:TaxAmount', self.NAMESPACES)
                        taxable_amount = self.safe_find_text(tax_subtotal, './/cbc:TaxableAmount', self.NAMESPACES)

                        if tax_code == '1000': invoice_data['total_igv'] = tax_amount; invoice_data['total_valor_venta'] = taxable_amount
                        elif tax_code == '2000': invoice_data['total_isc'] = tax_amount
                        elif tax_code == '9999': invoice_data['total_otros_tributos'] = tax_amount
                        elif tax_code == '9997': invoice_data['total_exonerado'] = taxable_amount
                        elif tax_code == '9998': invoice_data['total_inafecto'] = taxable_amount
                        elif tax_code == '9996': invoice_data['total_gratuito'] = taxable_amount
            
            invoice_data['indicador_retencion'] = self._extract_retencion_indicator(root)
            invoice_data['indicador_detraccion'] = self._extract_detraccion_indicator(root)

            cui = self._generate_cui(invoice_data)
            invoice_data['CUI'] = cui
            
            result['header'] = pd.DataFrame([invoice_data])

            lines = root.findall('.//cac:InvoiceLine', self.NAMESPACES)
            if lines:
                lines_list = [self._process_line(line, cui) for line in lines]
                result['lines'] = pd.DataFrame(lines_list)

            payment_terms_nodes = root.findall('.//cac:PaymentTerms', self.NAMESPACES)
            cuotas_de_pago = []

            if payment_terms_nodes:
                for pt_node in payment_terms_nodes:
                    # AÑADIR ESTE FILTRO:
                    # Si el ID del nodo NO es 'Detraccion', entonces es una cuota de pago real.
                    id_del_nodo = self.safe_find_text(pt_node, './cbc:ID', self.NAMESPACES)
                    if id_del_nodo != 'Detraccion':
                        # Solo si no es una detracción, lo procesamos como una cuota.
                        cuotas_de_pago.append(self._process_payment_term(pt_node, cui))

                if cuotas_de_pago:
                    result['payment_terms'] = pd.DataFrame(cuotas_de_pago)
            
            despatch_references = root.findall('.//cac:DespatchDocumentReference', self.NAMESPACES)
            if despatch_references:
                dr_list = []
                for dr in despatch_references:
                    dr_data = {'CUI': cui}
                    dr_data['guia_numero'] = self.safe_find_text(dr, './/cbc:ID', self.NAMESPACES)
                    dr_data['guia_tipo_documento'] = self.safe_find_text(dr, './/cbc:DocumentTypeCode', self.NAMESPACES)
                    dr_list.append(dr_data)
                result['despatch_references'] = pd.DataFrame(dr_list)

            self.log_operation("Procesamiento", "Éxito", f"Archivo: {file_name}, CUI: {cui}")
            return result
            
        except Exception as e:
            self.log_operation("Procesamiento", "Error", f"Error procesando archivo: {file_name}, Error: {str(e)}", level=logging.ERROR)
            return None

    def _generate_cui(self, data):
        ruc_emisor = data.get('ruc_emisor')
        tipo_doc = data.get('tipo_documento')
        numero_factura = data.get('numero')
        if ruc_emisor and tipo_doc and numero_factura:
            try:
                return f"{hex(int(ruc_emisor))[2:].lower()}{int(tipo_doc):02d}{numero_factura.replace('-', '')}"
            except (ValueError, TypeError):
                self.logger.warning(f"No se pudo generar CUI para {numero_factura}")
        return None

    def _process_line(self, line, cui):
        line_data = {'CUI': cui}
        line_data['linea_id'] = self.safe_find_text(line, './/cbc:ID', self.NAMESPACES)
        line_data['cantidad'] = self.safe_find_text(line, './/cbc:InvoicedQuantity', self.NAMESPACES)
        line_data['unidad'] = self.safe_find_attr(line, './/cbc:InvoicedQuantity', 'unitCode', self.NAMESPACES)
        line_data['descripcion'] = self.safe_find_text(line, './/cac:Item/cbc:Description', self.NAMESPACES)
        line_data['codigo_producto'] = self.safe_find_text(line, './/cac:Item/cac:SellersItemIdentification/cbc:ID', self.NAMESPACES)
        line_data['precio_unitario'] = self.safe_find_text(line, './/cac:Price/cbc:PriceAmount', self.NAMESPACES)
        line_data['subtotal'] = self.safe_find_text(line, './/cbc:LineExtensionAmount', self.NAMESPACES)
        tax_total = line.find('.//cac:TaxTotal', self.NAMESPACES)
        if tax_total:
            tax_subtotal = tax_total.find('.//cac:TaxSubtotal', self.NAMESPACES)
            if tax_subtotal:
                line_data['linea_igv'] = self.safe_find_text(tax_subtotal, './/cbc:TaxAmount', self.NAMESPACES)
                line_data['linea_igv_porcentaje'] = self.safe_find_text(tax_subtotal, './/cac:TaxCategory/cbc:Percent', self.NAMESPACES)
        return line_data

    def _process_payment_term(self, pt_node, cui):
        pt_data = {'CUI': cui}
        pt_data['forma_pago'] = self.safe_find_text(pt_node, './cbc:PaymentMeansID', self.NAMESPACES)
        pt_data['monto_pago'] = self.safe_find_text(pt_node, './cbc:Amount', self.NAMESPACES)
        pt_data['moneda_pago'] = self.safe_find_attr(pt_node, './cbc:Amount', 'currencyID', self.NAMESPACES)
        pt_data['fecha_vencimiento'] = self.safe_find_text(pt_node, './cbc:PaymentDueDate', self.NAMESPACES)
        return pt_data

    def _extract_retencion_indicator(self, root) -> bool:
        """Busca el indicador de retención del 3%."""
        allowance_charges = root.findall('.//cac:AllowanceCharge', self.NAMESPACES)
        for charge in allowance_charges:
            indicator = self.safe_find_text(charge, './cbc:ChargeIndicator', self.NAMESPACES)
            factor = self.safe_find_text(charge, './cbc:MultiplierFactorNumeric', self.NAMESPACES)
            if indicator == 'false' and factor == '0.03':
                return True
        return False

    def _extract_detraccion_indicator(self, root) -> Optional[str]:
        """Busca el código de detracción si existe."""
        payment_means = root.findall('.//cac:PaymentMeans', self.NAMESPACES)
        for pm in payment_means:
            pm_id = self.safe_find_text(pm, './cbc:ID', self.NAMESPACES)
            if pm_id == 'Detraccion':
                # Si encontramos el PaymentMeans de Detraccion, buscamos el PaymentTerms correspondiente
                payment_terms = root.findall('.//cac:PaymentTerms', self.NAMESPACES)
                for pt in payment_terms:
                    pt_id = self.safe_find_text(pt, './cbc:ID', self.NAMESPACES)
                    if pt_id == 'Detraccion':
                        return self.safe_find_text(pt, './cbc:PaymentMeansID', self.NAMESPACES)
        return None
